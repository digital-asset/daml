// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.{AssignmentViewType, TransactionViewType}
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.error.MediatorError.ParticipantEquivocation
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.Verdict.{Approve, MediatorReject}
import com.digitalasset.canton.sequencing.client.TestSequencerClientSend
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.mediator.store.{
  InMemoryFinalizedResponseStore,
  InMemoryMediatorDeduplicationStore,
  MediatorState,
}
import com.digitalasset.canton.synchronizer.metrics.MediatorTestMetrics
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.MonadUtil.{sequentialTraverse, sequentialTraverse_}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.HasTestCloseContext
import com.google.protobuf.ByteString
import io.grpc.Status.Code
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util
import scala.annotation.nowarn
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.language.reflectiveCalls

import ResponseAggregation.ConsortiumVotingState

@nowarn("msg=match may not be exhaustive")
class ConfirmationRequestAndResponseProcessorTest
    extends AsyncWordSpec
    with BaseTest
    with HasTestCloseContext
    with HasExecutionContext
    with FailOnShutdown {

  protected val synchronizerId: PhysicalSynchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::test")
  ).toPhysical
  protected val activeMediator1 = MediatorId(UniqueIdentifier.tryCreate("mediator", "one"))
  protected val activeMediator2 = MediatorId(UniqueIdentifier.tryCreate("mediator", "two"))
  protected val passiveMediator3 = MediatorId(UniqueIdentifier.tryCreate("mediator", "three"))
  protected val activeMediator4 = MediatorId(UniqueIdentifier.tryCreate("mediator", "four"))

  protected val testTopologyTimestamp = CantonTimestamp.Epoch

  private def mediatorGroup0(mediators: NonEmpty[Seq[MediatorId]]) =
    MediatorGroup(MediatorGroupIndex.zero, mediators, Seq.empty, PositiveInt.one)

  protected val mediatorGroup: MediatorGroup = MediatorGroup(
    index = MediatorGroupIndex.zero,
    active = Seq(activeMediator1, activeMediator2),
    passive = Seq(passiveMediator3),
    threshold = PositiveInt.tryCreate(2),
  )
  protected val mediatorGroup2: MediatorGroup = MediatorGroup(
    index = MediatorGroupIndex.one,
    active = Seq(activeMediator4),
    passive = Seq.empty,
    threshold = PositiveInt.one,
  )

  protected val sequencer = SequencerId(UniqueIdentifier.tryCreate("sequencer", "one"))

  protected val sequencerGroup =
    SequencerGroup(active = Seq(sequencer), Seq.empty, PositiveInt.one)

  private lazy val mediatorId: MediatorId = activeMediator2
  private lazy val mediatorGroupRecipient: MediatorGroupRecipient = MediatorGroupRecipient(
    mediatorGroup.index
  )
  private def addRecipients(
      request: MediatorConfirmationRequest,
      recipients: Recipients = Recipients.cc(mediatorGroupRecipient),
  ): OpenEnvelope[MediatorConfirmationRequest] =
    OpenEnvelope(request, recipients)(testedProtocolVersion)

  protected lazy val factory: ExampleTransactionFactory =
    new ExampleTransactionFactory()(
      psid = synchronizerId,
      mediatorGroup = mediatorGroupRecipient,
    )
  protected lazy val fullInformeeTree: FullInformeeTree =
    factory.MultipleRootsAndViewNestings.fullInformeeTree
  private lazy val view: TransactionView = factory.MultipleRootsAndViewNestings.view0
  protected val participant: ParticipantId = ExampleTransactionFactory.submittingParticipant

  protected lazy val view0Position =
    factory.MultipleRootsAndViewNestings.transactionViewTree0.viewPosition
  private lazy val view1Position =
    factory.MultipleRootsAndViewNestings.transactionViewTree1.viewPosition
  private lazy val view10Position =
    factory.MultipleRootsAndViewNestings.transactionViewTree10.viewPosition
  private lazy val view11Position =
    factory.MultipleRootsAndViewNestings.transactionViewTree11.viewPosition
  private lazy val view110Position =
    factory.MultipleRootsAndViewNestings.transactionViewTree110.viewPosition

  protected val notSignificantCounter: SequencerCounter = SequencerCounter(0)

  protected val initialSynchronizerParameters: DynamicSynchronizerParameters =
    TestSynchronizerParameters.defaultDynamic

  protected val initialSynchronizerParametersWithValidity =
    DynamicSynchronizerParametersWithValidity(
      initialSynchronizerParameters,
      CantonTimestamp.Epoch,
      None,
    )

  protected val confirmationResponseTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfMillis(100L)

  protected lazy val submitter = ExampleTransactionFactory.submitter
  protected lazy val signatory = ExampleTransactionFactory.signatory
  protected lazy val observer = ExampleTransactionFactory.observer
  protected lazy val extra: LfPartyId = ExampleTransactionFactory.extra

  // Create a topology with several participants so that we can have several root hash messages or Malformed messages
  protected val participant1 = participant
  protected val participant2 = ExampleTransactionFactory.signatoryParticipant
  protected val participant3 = ParticipantId("participant3")

  private lazy val crypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)

  private lazy val topology: TestingTopology = TestingTopology.from(
    synchronizers = Set(synchronizerId),
    topology = Map(
      submitter -> Map(participant -> ParticipantPermission.Confirmation),
      signatory ->
        Map(participant -> ParticipantPermission.Confirmation),
      observer ->
        Map(participant -> ParticipantPermission.Observation),
      extra ->
        Map(participant -> ParticipantPermission.Observation),
    ),
    mediatorGroups = Set(mediatorGroup, mediatorGroup2),
    sequencerGroup = sequencerGroup,
  )

  private lazy val identityFactory = TestingIdentityFactory(
    topology,
    loggerFactory,
    dynamicSynchronizerParameters = initialSynchronizerParameters
      .tryUpdate(confirmationResponseTimeout = confirmationResponseTimeout),
    crypto,
  )

  private lazy val identityFactory2 = {
    val topology2 = TestingTopology.from(
      synchronizers = Set(synchronizerId),
      topology = Map(
        submitter -> Map(participant1 -> ParticipantPermission.Confirmation),
        signatory -> Map(participant2 -> ParticipantPermission.Confirmation),
        observer -> Map(participant3 -> ParticipantPermission.Confirmation),
      ),
      mediatorGroups = Set(mediatorGroup),
      sequencerGroup = sequencerGroup,
    )
    TestingIdentityFactory(
      topology2,
      loggerFactory,
      initialSynchronizerParameters,
      crypto,
    )
  }

  private lazy val identityFactory3 = {
    val otherMediatorId = MediatorId(UniqueIdentifier.tryCreate("mediator", "other"))
    val topology3 =
      topology.copy(mediatorGroups = Set(mediatorGroup0(NonEmpty.mk(Seq, otherMediatorId))))
    TestingIdentityFactory(
      topology3,
      loggerFactory,
      dynamicSynchronizerParameters = initialSynchronizerParameters,
      crypto,
    )
  }

  private lazy val identityFactory4 = {
    val topology4 = TestingTopology(
      synchronizers = Set(synchronizerId),
      mediatorGroups = Set(mediatorGroup),
      sequencerGroup = sequencerGroup,
    )
      .withThreshold(
        Map(
          submitter -> (PositiveInt.one, Seq(participant1 -> ParticipantPermission.Confirmation)),
          signatory -> (PositiveInt.two, Seq(
            participant1 -> ParticipantPermission.Confirmation,
            participant2 -> ParticipantPermission.Confirmation,
            participant3 -> ParticipantPermission.Confirmation,
          )),
          observer -> (PositiveInt.one, Seq(participant3 -> ParticipantPermission.Confirmation)),
        )
      )

    TestingIdentityFactory(
      topology4,
      loggerFactory,
      initialSynchronizerParameters,
      crypto,
    )
  }

  private lazy val identityFactoryOnlySubmitter =
    TestingIdentityFactory(
      TestingTopology.from(
        synchronizers = Set(synchronizerId),
        topology = Map(
          submitter -> Map(participant1 -> ParticipantPermission.Confirmation)
        ),
        mediatorGroups = Set(mediatorGroup0(NonEmpty.mk(Seq, mediatorId))),
        sequencerGroup = sequencerGroup,
      ),
      loggerFactory,
      dynamicSynchronizerParameters = initialSynchronizerParameters,
      crypto,
    )

  protected lazy val synchronizerSyncCryptoApi: SynchronizerCryptoClient =
    identityFactory.forOwnerAndSynchronizer(mediatorId, synchronizerId)

  protected lazy val requestIdTs = CantonTimestamp.Epoch
  protected lazy val requestId = RequestId(requestIdTs)
  protected lazy val participantResponseDeadline = requestIdTs.plusSeconds(60)
  protected lazy val decisionTime = requestIdTs.plusSeconds(120)

  class Fixture(syncCryptoApi: SynchronizerCryptoClient = synchronizerSyncCryptoApi) {
    private val sequencerSend: TestSequencerClientSend = new TestSequencerClientSend

    def drainInterceptedBatches(): List[Batch[DefaultOpenEnvelope]] = {
      val result = new util.ArrayList[TestSequencerClientSend.Request]()
      sequencerSend.requestsQueue.drainTo(result)
      result.asScala.map(_.batch).toList
    }

    val verdictSender: TestVerdictSender =
      new TestVerdictSender(
        syncCryptoApi,
        mediatorId,
        sequencerSend,
        loggerFactory,
      )
    private val timeTracker: SynchronizerTimeTracker = mock[SynchronizerTimeTracker]
    val mediatorState = new MediatorState(
      new InMemoryFinalizedResponseStore(loggerFactory),
      new InMemoryMediatorDeduplicationStore(loggerFactory, timeouts),
      mock[Clock],
      MediatorTestMetrics,
      testedProtocolVersion,
      timeouts,
      loggerFactory,
    )
    mediatorState.initialize(CantonTimestamp.MinValue).futureValueUS
    val processor = new ConfirmationRequestAndResponseProcessor(
      mediatorId,
      verdictSender,
      syncCryptoApi,
      timeTracker,
      mediatorState,
      loggerFactory,
      timeouts,
    )
  }

  private lazy val synchronizerSyncCryptoApi2: SynchronizerCryptoClient =
    identityFactory2.forOwnerAndSynchronizer(sequencer, synchronizerId)

  private def signedResponses(
      requestId: RequestId,
      responses: NonEmpty[Seq[ConfirmationResponse]],
  ): FutureUnlessShutdown[SignedProtocolMessage[ConfirmationResponses]] = {
    val confirmationResponses: ConfirmationResponses = ConfirmationResponses.tryCreate(
      requestId,
      fullInformeeTree.transactionId.toRootHash,
      factory.psid,
      participant,
      responses,
      testedProtocolVersion,
    )
    val participantCrypto = identityFactory.forOwner(participant)
    SignedProtocolMessage
      .trySignAndCreate(
        confirmationResponses,
        participantCrypto
          .tryForSynchronizer(synchronizerId, defaultStaticSynchronizerParameters)
          .currentSnapshotApproximation,
      )
  }

  private def createConfirmationResponses(
      viewPosition: Option[ViewPosition],
      participant: ParticipantId,
      parties: Set[LfPartyId],
      verdict: LocalVerdict,
      identity: TestingIdentityFactory = identityFactory,
      requestId: RequestId = requestId,
  ): Future[SignedProtocolMessage[ConfirmationResponses]] = {
    val response = ConfirmationResponses.tryCreate(
      requestId,
      fullInformeeTree.transactionId.toRootHash,
      factory.psid,
      participant,
      NonEmpty.mk(
        Seq,
        ConfirmationResponse.tryCreate(
          viewPosition,
          verdict,
          parties,
        ),
      ),
      testedProtocolVersion,
    )

    val participantCrypto = identity.forOwner(participant)
    SignedProtocolMessage
      .trySignAndCreate(
        response,
        participantCrypto
          .tryForSynchronizer(synchronizerId, defaultStaticSynchronizerParameters)
          .currentSnapshotApproximation,
      )
      .failOnShutdown
  }

  private def sign(tree: FullInformeeTree): Signature = identityFactory
    .forOwnerAndSynchronizer(participant, synchronizerId)
    .awaitSnapshot(CantonTimestamp.Epoch)
    .futureValueUS
    .sign(tree.tree.rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
    .failOnShutdown
    .futureValue

  private def recipientFor(participants: Seq[ParticipantId]): Recipients =
    Recipients.recipientGroups(NonEmptyUtil.fromUnsafe(participants).map { participantId =>
      NonEmpty(Set, mediatorGroupRecipient, MemberRecipient(participantId))
    })

  "ConfirmationRequestAndResponseProcessor" should {
    def shouldBeViewThresholdBelowMinimumAlarm(
        requestId: RequestId,
        viewPosition: ViewPosition,
    ): LogEntry => Assertion =
      _.shouldBeCantonError(
        MediatorError.MalformedMessage,
        _ shouldBe s"Received a mediator confirmation request with id $requestId for transaction view at $viewPosition, " +
          s"where no quorum of the list satisfies the minimum threshold. Rejecting request...",
      )

    lazy val rootHashMessages = Seq(
      OpenEnvelope(
        RootHashMessage(
          fullInformeeTree.tree.rootHash,
          synchronizerId,
          TransactionViewType,
          testTopologyTimestamp,
          SerializedRootHashMessagePayload.empty,
        ),
        Recipients.cc(MemberRecipient(participant), mediatorGroupRecipient),
      )(testedProtocolVersion)
    )

    "timestamp of mediator confirmation request is propagated" in {
      val sut = new Fixture()

      val testMediatorRequest =
        new InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion) {
          val (firstFaultyViewPosition: ViewPosition, _) =
            super.informeesAndConfirmationParamsByViewPosition.head

          override def informeesAndConfirmationParamsByViewPosition
              : Map[ViewPosition, ViewConfirmationParameters] =
            super.informeesAndConfirmationParamsByViewPosition map {
              case (key, ViewConfirmationParameters(informees, quorums)) =>
                (
                  key,
                  ViewConfirmationParameters.tryCreate(
                    informees,
                    quorums.map(_.copy(threshold = NonNegativeInt.zero)),
                  ),
                )
            }
        }
      val requestTimestamp = CantonTimestamp.Epoch.plusSeconds(120)
      for {
        _ <- loggerFactory.assertLogs(
          sut.processor
            .processRequest(
              RequestId(requestTimestamp),
              SequencerCounter(0),
              requestTimestamp.plusSeconds(60),
              requestTimestamp.plusSeconds(120),
              addRecipients(testMediatorRequest),
              rootHashMessages,
              batchAlsoContainsTopologyTransaction = false,
            )
            .failOnShutdown,
          shouldBeViewThresholdBelowMinimumAlarm(
            RequestId(requestTimestamp),
            testMediatorRequest.firstFaultyViewPosition,
          ),
        )

      } yield {
        val sentResult = sut.verdictSender.sentResults.loneElement
        sentResult.requestId.unwrap shouldBe requestTimestamp
      }
    }

    "verify the request signature" in {
      val sut = new Fixture()

      val informeeMessage =
        new InformeeMessage(fullInformeeTree, Signature.noSignature)(testedProtocolVersion)

      for {
        _ <- loggerFactory.assertLogs(
          sut.processor
            .processRequest(
              requestId,
              notSignificantCounter,
              participantResponseDeadline,
              decisionTime,
              addRecipients(informeeMessage),
              rootHashMessages,
              batchAlsoContainsTopologyTransaction = false,
            )
            .failOnShutdown,
          _.shouldBeCantonError(
            MediatorError.MalformedMessage,
            _ should startWith(
              s"Received a mediator confirmation request with id $requestId from $participant with an invalid signature. Rejecting request.\nDetailed error: SignatureWithWrongKey"
            ),
          ),
        )
      } yield {
        val sentResult = sut.verdictSender.sentResults.loneElement
        inside(sentResult.verdict.value) { case MediatorReject(status, isMalformed) =>
          status.code shouldBe Code.INVALID_ARGUMENT.value()
          status.message shouldBe s"An error occurred. Please contact the operator and inquire about the request <no-correlation-id> with tid <no-tid>"
          isMalformed shouldBe true
        }
      }
    }

    "verify the response signature" in {
      val sut = new Fixture()

      val informeeMessage =
        new InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion)

      val requestTimestamp = CantonTimestamp.Epoch.plusSeconds(12345)
      val reqId = RequestId(requestTimestamp)

      for {
        _ <- sut.processor
          .processRequest(
            reqId,
            notSignificantCounter,
            requestTimestamp.plusSeconds(60),
            requestTimestamp.plusSeconds(120),
            addRecipients(informeeMessage),
            rootHashMessages,
            batchAlsoContainsTopologyTransaction = false,
          )
          .failOnShutdown
        response = ConfirmationResponses.tryCreate(
          reqId,
          fullInformeeTree.transactionId.toRootHash,
          factory.psid,
          participant,
          NonEmpty.mk(
            Seq,
            ConfirmationResponse.tryCreate(
              Some(view0Position),
              LocalApprove(testedProtocolVersion),
              Set(submitter),
            ),
          ),
          testedProtocolVersion,
        )
        signedResponse = SignedProtocolMessage(
          TypedSignedProtocolMessageContent(response),
          NonEmpty(Seq, Signature.noSignature),
        )
        _ <- loggerFactory.assertLogs(
          sut.processor
            .processResponses(
              CantonTimestamp.Epoch,
              notSignificantCounter,
              requestTimestamp.plusSeconds(60),
              requestTimestamp.plusSeconds(120),
              signedResponse,
              Some(reqId.unwrap),
              Recipients.cc(mediatorGroupRecipient),
            )
            .failOnShutdown,
          _.shouldBeCantonError(
            MediatorError.MalformedMessage,
            _ should include regex s"$synchronizerId \\(timestamp: ${CantonTimestamp.Epoch}\\): invalid signature from $participant with SignatureWithWrongKey",
          ),
        )
      } yield succeed
    }

    "accept root hash messages" in {
      val sut = new Fixture(synchronizerSyncCryptoApi2)
      val correctRootHash = fullInformeeTree.tree.rootHash
      // Create a custom informee message with several recipient participants
      val informeeMessage =
        new InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion) {
          override val informeesAndConfirmationParamsByViewPosition
              : Map[ViewPosition, ViewConfirmationParameters] =
            Map(
              ViewPosition.root -> ViewConfirmationParameters.tryCreate(
                Set(
                  submitter,
                  signatory,
                  observer,
                ),
                Seq(
                  Quorum(
                    Map(
                      submitter -> PositiveInt.one,
                      signatory -> PositiveInt.one,
                      observer -> PositiveInt.one,
                    ),
                    NonNegativeInt.one,
                  )
                ),
              )
            )
        }
      val allParticipants = NonEmpty(Seq, participant1, participant2, participant3)

      val correctViewType = informeeMessage.viewType
      val rootHashMessage =
        RootHashMessage(
          correctRootHash,
          synchronizerId,
          correctViewType,
          testTopologyTimestamp,
          SerializedRootHashMessagePayload.empty,
        )

      val tests = List[(String, Seq[Recipients])](
        "individual messages" -> allParticipants.map(p =>
          Recipients.cc(mediatorGroupRecipient, MemberRecipient(p))
        ),
        "just one message" -> Seq(
          Recipients.recipientGroups(
            allParticipants.map(p => NonEmpty.mk(Set, MemberRecipient(p), mediatorGroupRecipient))
          )
        ),
        "mixed" -> Seq(
          Recipients.recipientGroups(
            NonEmpty.mk(
              Seq,
              NonEmpty.mk(Set, MemberRecipient(participant1), mediatorGroupRecipient),
              NonEmpty.mk(Set, MemberRecipient(participant2), mediatorGroupRecipient),
            )
          ),
          Recipients.cc(MemberRecipient(participant3), mediatorGroupRecipient),
        ),
      )

      sequentialTraverse_(tests.zipWithIndex) { case ((testName, recipients), i) =>
        withClueF(testName) {
          val rootHashMessages =
            recipients.map(r => OpenEnvelope(rootHashMessage, r)(testedProtocolVersion))
          val ts = CantonTimestamp.ofEpochSecond(i.toLong)
          sut.processor
            .processRequest(
              RequestId(ts),
              notSignificantCounter,
              ts.plusSeconds(60),
              ts.plusSeconds(120),
              addRecipients(informeeMessage),
              rootHashMessages,
              batchAlsoContainsTopologyTransaction = false,
            )
            .failOnShutdown
        }
      }.map(_ => succeed)
    }

    "send rejections when receiving wrong root hash messages" in {
      val sut = new Fixture()

      val informeeMessage =
        InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion)
      val rootHash = informeeMessage.rootHash
      val wrongRootHash =
        RootHash(
          synchronizerSyncCryptoApi.pureCrypto.digest(TestHash.testHashPurpose, ByteString.EMPTY)
        )
      val correctViewType = informeeMessage.viewType
      val wrongViewType = AssignmentViewType
      require(correctViewType != wrongViewType)
      val correctRootHashMessage =
        RootHashMessage(
          rootHash,
          synchronizerId,
          correctViewType,
          testTopologyTimestamp,
          SerializedRootHashMessagePayload.empty,
        )
      val wrongRootHashMessage = correctRootHashMessage.copy(rootHash = wrongRootHash)
      val wrongViewTypeRHM = correctRootHashMessage.copy(viewType = wrongViewType)
      val otherParticipant = participant2

      def exampleForRequest(
          request: MediatorConfirmationRequest,
          rootHashMessages: (RootHashMessage[SerializedRootHashMessagePayload], Recipients)*
      ): (
          MediatorConfirmationRequest,
          List[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      ) =
        (
          request,
          rootHashMessages.map { case (rootHashMessage, recipients) =>
            OpenEnvelope(rootHashMessage, recipients)(testedProtocolVersion)
          }.toList,
        )

      def example(
          rootHashMessages: (RootHashMessage[SerializedRootHashMessagePayload], Recipients)*
      ) =
        exampleForRequest(informeeMessage, rootHashMessages*)

      val batchWithoutRootHashMessages = example()
      val batchWithWrongRootHashMessage =
        example(
          wrongRootHashMessage -> Recipients.cc(
            mediatorGroupRecipient,
            MemberRecipient(participant),
          )
        )
      val batchWithWrongViewType =
        example(
          wrongViewTypeRHM -> Recipients.cc(mediatorGroupRecipient, MemberRecipient(participant))
        )
      val batchWithDifferentViewTypes =
        example(
          correctRootHashMessage -> Recipients
            .cc(mediatorGroupRecipient, MemberRecipient(participant)),
          wrongViewTypeRHM -> Recipients.cc(
            mediatorGroupRecipient,
            MemberRecipient(otherParticipant),
          ),
        )
      val batchWithRootHashMessageWithTooManyRecipients =
        example(
          correctRootHashMessage -> Recipients.cc(
            mediatorGroupRecipient,
            MemberRecipient(participant),
            MemberRecipient(otherParticipant),
          )
        )
      val batchWithRootHashMessageWithTooFewRecipients =
        example(correctRootHashMessage -> Recipients.cc(mediatorGroupRecipient))
      val batchWithRepeatedRootHashMessage = example(
        correctRootHashMessage -> Recipients
          .cc(mediatorGroupRecipient, MemberRecipient(participant)),
        correctRootHashMessage -> Recipients.cc(
          mediatorGroupRecipient,
          MemberRecipient(participant),
        ),
      )
      val batchWithDivergingRootHashMessages = example(
        correctRootHashMessage -> Recipients
          .cc(mediatorGroupRecipient, MemberRecipient(participant)),
        wrongRootHashMessage -> Recipients.cc(
          mediatorGroupRecipient,
          MemberRecipient(participant),
        ),
      )
      val batchWithSuperfluousRootHashMessage = example(
        correctRootHashMessage -> Recipients
          .cc(mediatorGroupRecipient, MemberRecipient(participant)),
        correctRootHashMessage -> Recipients.cc(
          mediatorGroupRecipient,
          MemberRecipient(otherParticipant),
        ),
      )
      val batchWithDifferentPayloads = example(
        correctRootHashMessage -> Recipients
          .cc(mediatorGroupRecipient, MemberRecipient(participant)),
        correctRootHashMessage.copy(
          payload = SerializedRootHashMessagePayload(ByteString.copyFromUtf8("other payload"))
        ) -> Recipients
          .cc(mediatorGroupRecipient, MemberRecipient(otherParticipant)),
      )

      // format: off
      val testCases
      : Seq[(((MediatorConfirmationRequest, List[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]]), String),
        List[(Set[Member], ViewType)])] = List(

        (batchWithoutRootHashMessages -> show"Missing root hash message for informee participants: $participant") -> List.empty,

        (batchWithWrongRootHashMessage -> show"Wrong root hashes: $wrongRootHash") ->
          List(Set[Member](participant) -> correctViewType),

        (batchWithWrongViewType -> show"View types in root hash messages differ from expected view type $correctViewType: $wrongViewType") ->
          List(Set[Member](participant) -> wrongViewType),

        (batchWithDifferentViewTypes -> show"View types in root hash messages differ from expected view type $correctViewType: $wrongViewType") ->
          List(Set[Member](participant) -> correctViewType, Set[Member](otherParticipant) -> wrongViewType),

        (batchWithRootHashMessageWithTooManyRecipients ->
          show"Root hash messages with wrong recipients tree: RecipientsTree(recipient group = Seq($mediatorGroupRecipient, ${MemberRecipient(participant)}, ${MemberRecipient(otherParticipant)}))") ->
          List(Set[Member](participant, otherParticipant) -> correctViewType),

        (batchWithRootHashMessageWithTooFewRecipients -> show"Root hash messages with wrong recipients tree: RecipientsTree(recipient group = $mediatorGroupRecipient)") -> List.empty,

        (batchWithRepeatedRootHashMessage -> show"Several root hash messages for recipients: ${MemberRecipient(participant)}") ->
          List(Set[Member](participant) -> correctViewType),

        (batchWithDivergingRootHashMessages -> show"Several root hash messages for recipients: ${MemberRecipient(participant)}") ->
          List(Set[Member](participant) -> correctViewType),

        (batchWithSuperfluousRootHashMessage -> show"Superfluous root hash message for members: $otherParticipant") ->
          List(Set[Member](participant, otherParticipant) -> correctViewType),

        (batchWithDifferentPayloads -> show"Different payloads in root hash messages. Sizes: 0, 13.") ->
          List(Set[Member](participant, otherParticipant) -> correctViewType),
      )
      // format: on

      sequentialTraverse_(testCases.zipWithIndex) {
        case ((((req, rootHashMessages), msg), expectedRecipientsAndViewTypes), sc) =>
          val ts = CantonTimestamp.ofEpochSecond(sc.toLong)
          withClueF(s"at test case #$sc") {
            for {
              _ <- loggerFactory.assertLogs(
                // This will not send a result message because there are no root hash messages in the batch.
                sut.processor
                  .processRequest(
                    RequestId(ts),
                    notSignificantCounter,
                    ts.plusSeconds(60),
                    ts.plusSeconds(120),
                    addRecipients(req),
                    rootHashMessages,
                    batchAlsoContainsTopologyTransaction = false,
                  )
                  .failOnShutdown,
                _.shouldBeCantonError(
                  MediatorError.MalformedMessage,
                  _ shouldBe s"Received a mediator confirmation request with id ${RequestId(ts)} with invalid root hash messages. Rejecting... Reason: $msg",
                ),
              )
            } yield {
              val resultBatches = sut.drainInterceptedBatches()
              val resultRecipientsAndViewTypes = resultBatches.flatMap { resultBatch =>
                val envelope = resultBatch.envelopes.loneElement
                val viewType = envelope.protocolMessage
                  .asInstanceOf[SignedProtocolMessage[ConfirmationResultMessage]]
                  .message
                  .viewType

                envelope.recipients.trees.map(_ -> viewType)
              }.toSet

              val expected = expectedRecipientsAndViewTypes.flatMap { case (recipients, viewType) =>
                recipients.map { member =>
                  RecipientsTree.leaf(NonEmpty(Set, member)) -> viewType
                }
              }.toSet

              resultRecipientsAndViewTypes shouldBe expected
            }
          }
      }.map(_ => succeed)
    }

    "reject when declared mediator is wrong" in {
      val sut = new Fixture()

      val otherMediatorGroupIndex = MediatorGroupRecipient(mediatorGroup2.index)
      val factoryOtherMediatorId =
        new ExampleTransactionFactory()(
          psid = synchronizerId,
          mediatorGroup = otherMediatorGroupIndex,
        )
      val fullInformeeTreeOther =
        factoryOtherMediatorId.MultipleRootsAndViewNestings.fullInformeeTree
      val mediatorRequest =
        InformeeMessage(fullInformeeTreeOther, sign(fullInformeeTreeOther))(testedProtocolVersion)
      val rootHashMessage = RootHashMessage(
        mediatorRequest.rootHash,
        synchronizerId,
        mediatorRequest.viewType,
        testTopologyTimestamp,
        SerializedRootHashMessagePayload.empty,
      )

      val sc = 10L
      val ts = CantonTimestamp.ofEpochSecond(sc)
      for {
        _ <- loggerFactory.assertLogs(
          sut.processor
            .processRequest(
              RequestId(ts),
              notSignificantCounter,
              ts.plusSeconds(60),
              ts.plusSeconds(120),
              addRecipients(mediatorRequest),
              List(
                OpenEnvelope(
                  rootHashMessage,
                  Recipients.cc(mediatorGroupRecipient, MemberRecipient(participant)),
                )(testedProtocolVersion)
              ),
              batchAlsoContainsTopologyTransaction = false,
            )
            .failOnShutdown,
          _.shouldBeCantonError(
            MediatorError.MalformedMessage,
            message => {
              message should (include("Rejecting mediator confirmation request") and include(
                s"${RequestId(ts)}"
              ) and include("this mediator not being part of the mediator group") and include(
                s"$otherMediatorGroupIndex"
              ))
            },
          ),
        )
      } yield succeed
    }

    "reject when the recipients are wrong" in {
      val sut = new Fixture()

      val informeeMessage =
        new InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion)
      val wrongRecipients = Recipients.cc(MediatorGroupRecipient(MediatorGroupIndex.one))
      val expectedRecipients = Recipients.cc(mediatorGroupRecipient)

      for {
        _ <- loggerFactory.assertLogs(
          sut.processor
            .processRequest(
              requestId,
              notSignificantCounter,
              participantResponseDeadline,
              decisionTime,
              addRecipients(
                informeeMessage,
                wrongRecipients,
              ),
              rootHashMessages,
              batchAlsoContainsTopologyTransaction = false,
            )
            .failOnShutdown,
          _.shouldBeCantonError(
            MediatorError.MalformedMessage,
            _ shouldBe s"Rejecting mediator confirmation request with $requestId, mediator $mediatorGroupRecipient, topology at $requestIdTs due to wrong recipients (expected $expectedRecipients, actual $wrongRecipients)",
          ),
        )
      } yield {
        val sentResult = sut.verdictSender.sentResults.loneElement
        inside(sentResult.verdict.value) { case MediatorReject(status, isMalformed) =>
          status.code shouldBe Code.INVALID_ARGUMENT.value()
          status.message shouldBe s"An error occurred. Please contact the operator and inquire about the request <no-correlation-id> with tid <no-tid>"
          isMalformed shouldBe true
        }
      }
    }

    "correct series of mediator events" in {
      val sut = new Fixture()
      val informeeMessage =
        InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion)
      val rootHashMessage = RootHashMessage(
        fullInformeeTree.transactionId.toRootHash,
        synchronizerId,
        ViewType.TransactionViewType,
        testTopologyTimestamp,
        SerializedRootHashMessagePayload.empty,
      )
      val mockTopologySnapshot = mock[TopologySnapshot]
      when(
        mockTopologySnapshot.canConfirm(any[ParticipantId], any[Set[LfPartyId]])(
          anyTraceContext
        )
      )
        .thenAnswer { (_: ParticipantId, parties: Set[LfPartyId]) =>
          FutureUnlessShutdown.pure(parties)
        }
      when(
        mockTopologySnapshot.activeParticipantsOfPartiesWithInfo(any[Seq[LfPartyId]])(
          anyTraceContext
        )
      )
        .thenAnswer { (parties: Seq[LfPartyId]) =>
          FutureUnlessShutdown.pure(
            parties
              .map(party =>
                party -> PartyInfo(
                  PositiveInt.one,
                  Map(ParticipantId("one") -> ParticipantAttributes(Confirmation)),
                )
              )
              .toMap
          )
        }

      for {
        _ <- sut.processor
          .processRequest(
            requestId,
            notSignificantCounter,
            requestIdTs.plusSeconds(60),
            decisionTime,
            addRecipients(informeeMessage),
            List(
              OpenEnvelope(
                rootHashMessage,
                Recipients.cc(mediatorGroupRecipient, MemberRecipient(participant)),
              )(
                testedProtocolVersion
              )
            ),
            batchAlsoContainsTopologyTransaction = false,
          )
        // should record the request
        requestState <- sut.mediatorState
          .fetch(requestId)
          .value
          .map(_.value)
        responseAggregation <-
          ResponseAggregation.fromRequest(
            requestId,
            informeeMessage,
            participantResponseDeadline,
            decisionTime,
            mockTopologySnapshot,
          )

        _ = requestState shouldBe responseAggregation
        // receiving the confirmation response
        ts1 = CantonTimestamp.Epoch.plusMillis(1L)
        responses = List(
          view0Position -> view,
          view1Position -> factory.MultipleRootsAndViewNestings.view1,
          view11Position -> factory.MultipleRootsAndViewNestings.view11,
          view110Position -> factory.MultipleRootsAndViewNestings.view110,
        ).foldLeft(Seq.empty[ConfirmationResponse]) { case (acc, (viewPosition, _)) =>
          acc :+ ConfirmationResponse.tryCreate(
            Some(viewPosition),
            LocalApprove(testedProtocolVersion),
            Set(submitter),
          )
        }
        responsesNE = NonEmptyUtil.fromUnsafe(responses)
        approval <- signedResponses(requestId, responsesNE)
        _ <- sut.processor
          .processResponses(
            ts1,
            notSignificantCounter,
            ts1.plusSeconds(60),
            ts1.plusSeconds(120),
            approval,
            Some(requestId.unwrap),
            Recipients.cc(mediatorGroupRecipient),
          )
        // records the request
        updatedState <- sut.mediatorState
          .fetch(requestId)
          .value
        _ = {
          inside(updatedState) {
            case Some(
                  ResponseAggregation(
                    actualRequestId,
                    actualRequest,
                    _,
                    _,
                    actualVersion,
                    Right(_states),
                  )
                ) =>
              actualRequestId shouldBe requestId
              actualRequest shouldBe informeeMessage
              actualVersion shouldBe ts1
          }
          val completedView = ResponseAggregation.ViewState(
            Map(
              submitter -> ConsortiumVotingState.withDefaultValues(approvals =
                Set(ExampleTransactionFactory.submittingParticipant)
              )
            ),
            Seq(Quorum.empty),
            Nil,
          )
          val signatoryQuorum = Quorum(
            Map(signatory -> PositiveInt.one),
            NonNegativeInt.one,
          )
          val ResponseAggregation(
            `requestId`,
            `informeeMessage`,
            _,
            _,
            `ts1`,
            Right(states),
          ) = updatedState.value
          assert(
            states === Map(
              view0Position -> completedView,
              view1Position ->
                ResponseAggregation.ViewState(
                  Map(
                    submitter -> ConsortiumVotingState.withDefaultValues(approvals =
                      Set(ExampleTransactionFactory.submittingParticipant)
                    ),
                    signatory -> ConsortiumVotingState.withDefaultValues(),
                  ),
                  Seq(signatoryQuorum),
                  Nil,
                ),
              view10Position ->
                ResponseAggregation.ViewState(
                  Map(signatory -> ConsortiumVotingState.withDefaultValues()),
                  Seq(signatoryQuorum),
                  Nil,
                ),
              view11Position ->
                ResponseAggregation.ViewState(
                  Map(
                    submitter -> ConsortiumVotingState.withDefaultValues(approvals =
                      Set(ExampleTransactionFactory.submittingParticipant)
                    ),
                    signatory -> ConsortiumVotingState.withDefaultValues(),
                  ),
                  Seq(signatoryQuorum),
                  Nil,
                ),
              view110Position -> completedView,
            )
          )
        }
        // receiving the final confirmation response
        ts2 = CantonTimestamp.Epoch.plusMillis(2L)
        responses = List(
          view1Position -> factory.MultipleRootsAndViewNestings.view1,
          view10Position -> factory.MultipleRootsAndViewNestings.view10,
          view11Position -> factory.MultipleRootsAndViewNestings.view11,
        ).foldLeft(Seq.empty[ConfirmationResponse]) { case (acc, (viewPosition, _)) =>
          acc :+ ConfirmationResponse.tryCreate(
            Some(viewPosition),
            LocalApprove(testedProtocolVersion),
            Set(signatory),
          )
        }
        responsesNE = NonEmptyUtil.fromUnsafe(responses)
        approval <- signedResponses(requestId, responsesNE)
        _ <- sut.processor
          .processResponses(
            ts2,
            notSignificantCounter,
            ts2.plusSeconds(60),
            ts2.plusSeconds(120),
            approval,
            Some(requestId.unwrap),
            Recipients.cc(mediatorGroupRecipient),
          )
        // records the request
        finalState <- sut.mediatorState
          .fetch(requestId)
          .value
      } yield {
        inside(finalState) {
          case Some(FinalizedResponse(`requestId`, `informeeMessage`, `ts2`, verdict)) =>
            assert(verdict === Approve(testedProtocolVersion))
        }
      }
    }

    "receiving Malformed responses" in {
      val sut = new Fixture(synchronizerSyncCryptoApi2)

      // Create a custom informee message with many quorums such that the first Malformed rejection doesn't finalize the request
      val informeeMessage =
        new InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion) {
          override val informeesAndConfirmationParamsByViewPosition
              : Map[ViewPosition, ViewConfirmationParameters] = {
            val allNodesViewConfirmationParameters = ViewConfirmationParameters.tryCreate(
              Set(
                submitter,
                signatory,
                observer,
              ),
              Seq(
                Quorum(
                  Map(
                    submitter -> PositiveInt.one,
                    signatory -> PositiveInt.one,
                    observer -> PositiveInt.one,
                  ),
                  NonNegativeInt.one,
                )
              ),
            )
            Map(
              view0Position -> ViewConfirmationParameters.tryCreate(
                Set(
                  submitter,
                  signatory,
                ),
                Seq(
                  Quorum(
                    Map(
                      submitter -> PositiveInt.one,
                      signatory -> PositiveInt.one,
                    ),
                    NonNegativeInt.one,
                  )
                ),
              ),
              view1Position -> allNodesViewConfirmationParameters,
              view11Position -> ViewConfirmationParameters.tryCreate(
                Set(
                  observer,
                  signatory,
                ),
                Seq(
                  Quorum(
                    Map(
                      observer -> PositiveInt.one,
                      signatory -> PositiveInt.one,
                    ),
                    NonNegativeInt.one,
                  )
                ),
              ),
              view10Position -> allNodesViewConfirmationParameters,
            )
          }
        }

      val rootHashMessage = RootHashMessage(
        fullInformeeTree.transactionId.toRootHash,
        synchronizerId,
        ViewType.TransactionViewType,
        testTopologyTimestamp,
        SerializedRootHashMessagePayload.empty,
      )

      val requestIdTs = CantonTimestamp.Epoch
      val requestId = RequestId(requestIdTs)

      val malformedMsg = "this is a test malformed response"

      def malformedResponse(
          participant: ParticipantId
      ): Future[SignedProtocolMessage[ConfirmationResponses]] =
        createConfirmationResponses(
          None,
          participant,
          Set.empty,
          LocalRejectError.MalformedRejects.Payloads
            .Reject(malformedMsg)
            .toLocalReject(testedProtocolVersion),
          identityFactory2,
        )

      for {
        _ <- sut.processor
          .processRequest(
            requestId,
            notSignificantCounter,
            requestIdTs.plusSeconds(60),
            requestIdTs.plusSeconds(120),
            addRecipients(informeeMessage),
            List(
              OpenEnvelope(
                rootHashMessage,
                recipientFor(Seq(participant1, participant2, participant3)),
              )(
                testedProtocolVersion
              )
            ),
            batchAlsoContainsTopologyTransaction = false,
          )
          .failOnShutdown

        // receiving a confirmation response
        ts1 = CantonTimestamp.Epoch.plusMillis(1L)
        malformed <- sequentialTraverse(
          List(
            malformedResponse(participant1),
            malformedResponse(participant3),
            malformedResponse(participant3),
            malformedResponse(participant2), // This should finalize the request
            malformedResponse(participant3),
          )
        )(Predef.identity)

        // records the request
        _ <- sequentialTraverse_(malformed)(
          sut.processor
            .processResponses(
              ts1,
              notSignificantCounter,
              ts1.plusSeconds(60),
              ts1.plusSeconds(120),
              _,
              Some(requestId.unwrap),
              Recipients.cc(mediatorGroupRecipient),
            )
            .failOnShutdown("Unexpected shutdown.")
        )

        finalState <- sut.mediatorState
          .fetch(requestId)
          .value
          .failOnShutdown("Unexpected shutdown.")
        _ = inside(finalState) {
          case Some(
                FinalizedResponse(
                  _requestId,
                  _request,
                  _version,
                  Verdict.ParticipantReject(reasons),
                )
              ) =>
            // TODO(#5337) These are only the rejections for the first view because this view happens to be finalized first.
            reasons.length shouldEqual 2
            reasons.foreach { case (party, reject) =>
              reject shouldBe LocalRejectError.MalformedRejects.Payloads
                .Reject(malformedMsg)
                .toLocalReject(
                  testedProtocolVersion
                )
              party should (contain(submitter) or contain(signatory))
            }
        }
      } yield succeed
    }

    "raise an alarm when a participant send two contradictory verdicts" in {
      val sut = new Fixture(identityFactory4.forOwnerAndSynchronizer(sequencer, synchronizerId))
      val informeeMessage =
        new InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion) {
          override val informeesAndConfirmationParamsByViewPosition = Map(
            view0Position -> ViewConfirmationParameters.tryCreate(
              Set(signatory),
              Seq(
                Quorum(
                  Map(signatory -> PositiveInt.one),
                  NonNegativeInt.one,
                )
              ),
            )
          )
        }

      val rootHashMessage = RootHashMessage(
        fullInformeeTree.transactionId.toRootHash,
        synchronizerId,
        ViewType.TransactionViewType,
        testTopologyTimestamp,
        SerializedRootHashMessagePayload.empty,
      )

      def approve(participantId: ParticipantId) = createConfirmationResponses(
        Some(view0Position),
        participantId,
        Set(signatory),
        LocalApprove(testedProtocolVersion),
        identityFactory4,
      )
      def reject(participantId: ParticipantId) =
        createConfirmationResponses(
          Some(view0Position),
          participantId,
          Set(signatory),
          LocalRejectError.ConsistencyRejections.LockedContracts
            .Reject(Nil)
            .toLocalReject(testedProtocolVersion),
          identityFactory4,
        )

      val localAbstain = LocalAbstainError.CannotPerformAllValidations
        .Abstain("this is a test abstain response")
        .toLocalAbstain(testedProtocolVersion)

      def abstain(participantId: ParticipantId) =
        createConfirmationResponses(
          Some(view0Position),
          participantId,
          Set(signatory),
          localAbstain,
          identityFactory4,
        )

      def processResponse(
          ts: CantonTimestamp,
          verdict: SignedProtocolMessage[ConfirmationResponses],
      ): Future[Unit] =
        sut.processor
          .processResponses(
            ts,
            notSignificantCounter,
            ts.plusSeconds(60),
            ts.plusSeconds(120),
            verdict,
            Some(requestId.unwrap),
            Recipients.cc(mediatorGroupRecipient),
          )
          .failOnShutdown("Unexpected shutdown.")

      for {
        _ <- sut.processor
          .processRequest(
            requestId,
            notSignificantCounter,
            requestIdTs.plusSeconds(60),
            requestIdTs.plusSeconds(120),
            addRecipients(informeeMessage),
            List(
              OpenEnvelope(
                rootHashMessage,
                recipientFor(Seq(participant1, participant2, participant3)),
              )(
                testedProtocolVersion
              )
            ),
            batchAlsoContainsTopologyTransaction = false,
          )
          .failOnShutdown

        // receiving a confirmation response
        ts1 = CantonTimestamp.Epoch.plusMillis(1L)

        _ <- approve(participant1).flatMap(processResponse(ts1, _))
        // should be ignored
        _ <- abstain(participant1).flatMap(processResponse(ts1, _))

        // should raise an alarm
        _ <- loggerFactory.assertLogs(
          reject(participant1).flatMap(processResponse(ts1, _)),
          (log: LogEntry) => {
            log.shouldBeCantonErrorCode(ParticipantEquivocation.code)
            log.warningMessage should include(
              s"$requestId(view position ViewPosition(L)): Ignoring a rejection verdict for $participant1 because it has already responded for party $signatory with an approval verdict"
            )
          },
        )

        // same as before but this time we reject first and then abstain and then approve
        _ <- reject(participant2).flatMap(processResponse(ts1, _))
        // should be ignored
        _ <- abstain(participant2).flatMap(processResponse(ts1, _))

        // should raise an alarm
        _ <- loggerFactory.assertLogs(
          approve(participant2).flatMap(processResponse(ts1, _)),
          (log: LogEntry) => {
            log.shouldBeCantonErrorCode(ParticipantEquivocation.code)
            log.warningMessage should include(
              s"$requestId(view position ViewPosition(L)): Ignoring an approval verdict for $participant2 because it has already responded for party $signatory with a rejection verdict"
            )
          },
        )

        // finalize the request with an abstain verdict
        _ <- abstain(participant3).flatMap(processResponse(ts1, _))

        finalState <- sut.mediatorState
          .fetch(requestId)
          .value
          .failOnShutdown("Unexpected shutdown.")

        _ = inside(finalState) {
          case Some(
                FinalizedResponse(
                  _requestId,
                  _request,
                  _version,
                  Verdict.ParticipantReject(reasons),
                )
              ) =>
            reasons.length shouldEqual 1
            reasons.foreach { case (party, reject) =>
              reject shouldBe localAbstain
              party shouldBe Set(signatory)
            }
        }
      } yield succeed
    }

    "receiving abstain verdicts" in {
      val sut = new Fixture(identityFactory4.forOwnerAndSynchronizer(sequencer, synchronizerId))

      // Create a custom informee message with a quorum such that the first rejection or abstain doesn't finalize the request
      val informeeMessage =
        new InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion) {
          override val informeesAndConfirmationParamsByViewPosition = Map(
            view0Position -> ViewConfirmationParameters.tryCreate(
              Set(submitter, signatory, observer),
              Seq(
                Quorum(
                  Map(
                    submitter -> PositiveInt.one,
                    signatory -> PositiveInt.one,
                    observer -> PositiveInt.one,
                  ),
                  NonNegativeInt.two,
                )
              ),
            )
          )
        }

      val rootHashMessage = RootHashMessage(
        fullInformeeTree.transactionId.toRootHash,
        synchronizerId,
        ViewType.TransactionViewType,
        testTopologyTimestamp,
        SerializedRootHashMessagePayload.empty,
      )

      val abstainMsg = "this is a test abstain response"

      val localAbstain = LocalAbstainError.CannotPerformAllValidations
        .Abstain(abstainMsg)
        .toLocalAbstain(testedProtocolVersion)

      def createAbstain(
          viewPosition: ViewPosition,
          participant: ParticipantId,
          parties: Set[LfPartyId],
      ): Future[SignedProtocolMessage[ConfirmationResponses]] =
        createConfirmationResponses(
          Some(viewPosition),
          participant,
          parties,
          localAbstain,
          identityFactory4,
        )

      for {
        _ <- sut.processor
          .processRequest(
            requestId,
            notSignificantCounter,
            requestIdTs.plusSeconds(60),
            requestIdTs.plusSeconds(120),
            addRecipients(informeeMessage),
            List(
              OpenEnvelope(
                rootHashMessage,
                recipientFor(Seq(participant1, participant2, participant3)),
              )(testedProtocolVersion)
            ),
            batchAlsoContainsTopologyTransaction = false,
          )
          .failOnShutdown

        // receiving a confirmation response
        ts1 = CantonTimestamp.Epoch.plusMillis(1L)
        _ <- MonadUtil.sequentialTraverse(
          Seq(
            createAbstain(view0Position, participant2, Set(signatory)),
            // should be counted as a rejection for signatory
            createAbstain(view0Position, participant1, Set(signatory)),
            createAbstain(view0Position, participant1, Set(submitter)),
            // should be ignored
            createAbstain(view0Position, participant3, Set(observer)),
          )
        )(
          _.flatMap(abstain =>
            sut.processor
              .processResponses(
                ts1,
                notSignificantCounter,
                ts1.plusSeconds(60),
                ts1.plusSeconds(120),
                abstain,
                Some(requestId.unwrap),
                Recipients.cc(mediatorGroupRecipient),
              )
              .failOnShutdown("Unexpected shutdown.")
          )
        )

        finalState <- sut.mediatorState
          .fetch(requestId)
          .value
          .failOnShutdown("Unexpected shutdown.")

        _ = inside(finalState) {
          case Some(
                FinalizedResponse(
                  _requestId,
                  _request,
                  _version,
                  Verdict.ParticipantReject(reasons),
                )
              ) =>
            reasons.length shouldEqual 2
            reasons.foreach { case (party, reject) =>
              reject shouldBe localAbstain
              party should (contain(submitter) or contain(signatory))
            }
        }
      } yield succeed
    }

    "receiving late response" in {
      val sut = new Fixture()
      val requestTs = CantonTimestamp.Epoch.plusMillis(1)
      val requestId = RequestId(requestTs)
      // response is just too late
      val participantResponseDeadline = requestIdTs.plus(confirmationResponseTimeout.unwrap)
      val responseTs = participantResponseDeadline.addMicros(1)

      val informeeMessage =
        InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion)
      val rootHashMessage = RootHashMessage(
        fullInformeeTree.transactionId.toRootHash,
        synchronizerId,
        ViewType.TransactionViewType,
        testTopologyTimestamp,
        SerializedRootHashMessagePayload.empty,
      )

      for {
        _ <- sut.processor
          .processRequest(
            requestId,
            notSignificantCounter,
            requestIdTs.plus(confirmationResponseTimeout.unwrap),
            requestIdTs.plusSeconds(120),
            addRecipients(informeeMessage),
            List(
              OpenEnvelope(
                rootHashMessage,
                Recipients.cc(mediatorGroupRecipient, MemberRecipient(participant)),
              )(
                testedProtocolVersion
              )
            ),
            batchAlsoContainsTopologyTransaction = false,
          )
          .failOnShutdown
        response <- createConfirmationResponses(
          Some(ViewPosition.root),
          participant,
          Set(submitter),
          LocalApprove(testedProtocolVersion),
          requestId = requestId,
        )
        _ <- loggerFactory.assertLogs(
          sut.processor
            .processResponses(
              responseTs,
              notSignificantCounter + 1,
              participantResponseDeadline,
              requestIdTs.plusSeconds(120),
              response,
              Some(requestId.unwrap),
              Recipients.cc(mediatorGroupRecipient),
            ),
          _.warningMessage shouldBe s"Response $responseTs is too late as request RequestId($requestTs) has already exceeded the participant response deadline [$participantResponseDeadline]",
        )
      } yield succeed
    }

    "ignore responses for future request" in {
      val sut = new Fixture()
      val sequencingTs = requestId.unwrap.minusSeconds(1)

      for {
        response <- createConfirmationResponses(
          Some(ViewPosition.root),
          participant,
          Set(submitter),
          LocalApprove(testedProtocolVersion),
        )
        _ <- loggerFactory.assertLogs(
          sut.processor.handleMediatorEvent(
            MediatorEvent.Response(
              notSignificantCounter,
              sequencingTs,
              response,
              topologyTimestamp = Some(requestId.unwrap),
              recipients = Recipients.cc(mediatorGroupRecipient),
            )
          ),
          _.shouldBeCantonError(
            MediatorError.MalformedMessage,
            _ should include("with earlier sequencing time"),
          ),
        )
      } yield succeed
    }

    "reject requests whose batch contained a topology transaction" in {
      val sut = new Fixture()

      val mediatorRequest =
        InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion)
      val rootHashMessage = RootHashMessage(
        mediatorRequest.rootHash,
        synchronizerId,
        mediatorRequest.viewType,
        testTopologyTimestamp,
        SerializedRootHashMessagePayload.empty,
      )

      val sc = 10L
      val ts = CantonTimestamp.ofEpochSecond(sc)
      for {
        _ <- loggerFactory.assertLogs(
          sut.processor
            .processRequest(
              RequestId(ts),
              notSignificantCounter,
              ts.plusSeconds(60),
              ts.plusSeconds(120),
              addRecipients(mediatorRequest),
              List(
                OpenEnvelope(
                  rootHashMessage,
                  Recipients.cc(mediatorGroupRecipient, MemberRecipient(participant)),
                )(
                  testedProtocolVersion
                )
              ),
              batchAlsoContainsTopologyTransaction = true,
            )
            .failOnShutdown,
          _.shouldBeCantonError(
            MediatorError.MalformedMessage,
            message => {
              message should include(
                s"Received a mediator confirmation request with id ${RequestId(ts)} also containing a topology transaction."
              )
            },
          ),
        )
      } yield succeed
    }

    "timeout request that is not pending should not fail" in {
      // could happen if a timeout is scheduled but the request is previously finalized
      val sut = new Fixture()
      val requestTs = CantonTimestamp.Epoch
      val requestId = RequestId(requestTs)
      val timeoutTs = requestTs.plusSeconds(20)

      // this request is not added to the pending state
      for {
        snapshot <- synchronizerSyncCryptoApi2.snapshot(requestTs)
        _ <- sut.processor
          .handleTimeout(requestId, timeoutTs)
      } yield succeed
    }

    "reject request if some informee is not hosted by an active participant" in {
      val synchronizerSyncCryptoApi =
        identityFactoryOnlySubmitter.forOwnerAndSynchronizer(mediatorId, synchronizerId)
      val sut = new Fixture(synchronizerSyncCryptoApi)

      val request =
        InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion)

      for {
        _ <- loggerFactory.assertLogs(
          sut.processor
            .processRequest(
              requestId,
              notSignificantCounter,
              requestIdTs.plusSeconds(20),
              decisionTime,
              addRecipients(request),
              rootHashMessages,
              batchAlsoContainsTopologyTransaction = false,
            )
            .failOnShutdown,
          _.shouldBeCantonError(
            MediatorError.InvalidMessage,
            _ shouldBe s"Received a mediator confirmation request with id $requestId with some informees not being hosted by an active participant: ${Set(observer, extra, signatory)}. Rejecting request...",
          ),
        )
      } yield succeed
    }

    "inactive mediator ignores requests" in {
      val synchronizerSyncCryptoApi3 =
        identityFactory3.forOwnerAndSynchronizer(mediatorId, synchronizerId)
      val sut = new Fixture(synchronizerSyncCryptoApi3)

      val mediatorRequest =
        InformeeMessage(fullInformeeTree, sign(fullInformeeTree))(testedProtocolVersion)
      val rootHashMessage = RootHashMessage(
        mediatorRequest.rootHash,
        synchronizerId,
        mediatorRequest.viewType,
        testTopologyTimestamp,
        SerializedRootHashMessagePayload.empty,
      )

      val sc = SequencerCounter(100)
      val ts = CantonTimestamp.ofEpochSecond(sc.v)
      val requestId = RequestId(ts)
      for {
        _ <- sut.processor
          .processRequest(
            RequestId(ts),
            notSignificantCounter,
            ts.plusSeconds(60),
            ts.plusSeconds(120),
            addRecipients(mediatorRequest),
            List(
              OpenEnvelope(
                rootHashMessage,
                Recipients.cc(mediatorGroupRecipient, MemberRecipient(participant)),
              )(
                testedProtocolVersion
              )
            ),
            batchAlsoContainsTopologyTransaction = false,
          )
          .failOnShutdown
        _ = sut.verdictSender.sentResults shouldBe empty

        // If it nevertheless gets a response, it will complain about the request not being known
        response <- createConfirmationResponses(
          Some(ViewPosition.root),
          participant,
          Set(submitter),
          LocalApprove(testedProtocolVersion),
          requestId = requestId,
        )
        _ <- loggerFactory.assertLogs(
          sut.processor
            .processResponses(
              ts.immediateSuccessor,
              sc + 1L,
              ts.plusSeconds(60),
              ts.plusSeconds(120),
              response,
              Some(requestId.unwrap),
              Recipients.cc(mediatorGroupRecipient),
            ),
          _.shouldBeCantonError(
            MediatorError.InvalidMessage,
            _ shouldBe show"Received a confirmation response at ${ts.immediateSuccessor} by $participant with an unknown request id $requestId. Discarding response...",
          ),
        )
      } yield {
        succeed
      }
    }

    "check the timestamp of signing key on responses" in {
      val sut = new Fixture(synchronizerSyncCryptoApi2)

      val requestIdTs = CantonTimestamp.Epoch
      val requestId = RequestId(requestIdTs)

      def checkWrongTimestampOfSigningKeyError(logEntry: LogEntry): Assertion =
        logEntry.shouldBeCantonError(
          MediatorError.MalformedMessage,
          message =>
            message should (include(
              "Discarding confirmation response because the topology timestamp is not set to the request id"
            ) and include(s"$requestId")),
        )

      val ts1 = CantonTimestamp.Epoch.plusMillis(1L)
      for {
        someResponse <- createConfirmationResponses(
          Some(ViewPosition.root),
          participant,
          Set(submitter),
          LocalApprove(testedProtocolVersion),
        )

        _ <- loggerFactory.assertLogs(
          sut.processor
            .processResponses(
              ts1,
              notSignificantCounter,
              ts1.plusSeconds(60),
              ts1.plusSeconds(120),
              someResponse,
              // No timestamp of signing key given
              None,
              Recipients.cc(mediatorGroupRecipient),
            ),
          checkWrongTimestampOfSigningKeyError,
        )

        _ <- loggerFactory.assertLogs(
          sut.processor
            .processResponses(
              ts1,
              notSignificantCounter,
              ts1.plusSeconds(60),
              ts1.plusSeconds(120),
              someResponse,
              // Wrong timestamp of signing key given
              Some(requestId.unwrap.immediatePredecessor),
              Recipients.cc(mediatorGroupRecipient),
            ),
          checkWrongTimestampOfSigningKeyError,
        )
      } yield succeed
    }
  }
}
