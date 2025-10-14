// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import cats.syntax.functor.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.{
  CryptoPureApi,
  Salt,
  SigningKeyUsage,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.{AssignmentViewType, UnassignmentViewType}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.util.TestSubmissionService
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.{
  ViewHashAndRecipients,
  ViewKeyData,
}
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  SeedGenerator,
  TransactionConfirmationRequestFactory,
  TransactionTreeFactory,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.{SendResult, SequencerClient}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.SessionKeyStoreWithInMemoryCache
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LedgerCommandId, LfPartyId, WorkflowId, checked}
import com.digitalasset.daml.lf.data.Ref.UserId
import com.digitalasset.daml.lf.transaction.test.TestIdFactory
import org.scalatest.EitherValues.*
import org.scalatest.OptionValues.*

import java.time.Duration
import java.util.UUID
import scala.concurrent.ExecutionContext

/** Wrapper around a participant that allows for submitting invalid confirmation requests. */
class MaliciousParticipantNode(
    participantId: ParticipantId,
    testSubmissionService: TestSubmissionService,
    seedGenerator: SeedGenerator,
    contractOfId: TransactionTreeFactory.ContractInstanceOfId,
    confirmationRequestFactory: TransactionConfirmationRequestFactory,
    sequencerClient: SequencerClient,
    defaultPSId: PhysicalSynchronizerId,
    defaultMediatorGroup: MediatorGroupRecipient,
    pureCrypto: CryptoPureApi,
    defaultCryptoSnapshot: () => SynchronizerSnapshotSyncCryptoApi,
    defaultProtocolVersion: ProtocolVersion,
    timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends NamedLogging
    with TestIdFactory {

  val futureSupervisor: FutureSupervisor = FutureSupervisor.Noop

  private val transactionTreeFactory: TransactionTreeFactory =
    confirmationRequestFactory.transactionTreeFactory

  private def sendRequestBatchToSequencer(
      batch: Batch[DefaultOpenEnvelope],
      topologyTimestamp: Option[CantonTimestamp] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SendResult.Success] = {
    val promise = PromiseUnlessShutdown.unsupervised[Either[String, SendResult.Success]]()
    implicit val metricsContext: MetricsContext = MetricsContext.Empty
    for {
      _ <- sequencerClient
        .send(
          batch,
          topologyTimestamp = topologyTimestamp,
          callback = {
            case UnlessShutdown.Outcome(success: SendResult.Success) =>
              promise.outcome_(Right(success))
            case UnlessShutdown.Outcome(result: SendResult) =>
              promise.outcome_(Left(s"Sending request failed asynchronously: $result"))
            case UnlessShutdown.AbortedDueToShutdown =>
              promise.failure(new RuntimeException("Shutdown happened while test was running"))
          },
        )
        .leftMap(_.show)
      sendResult <- EitherT(promise.futureUS)
    } yield sendResult
  }

  def submitUnassignmentRequest(
      fullTree: FullUnassignmentTree,
      mediator: MediatorGroupRecipient = defaultMediatorGroup,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi = defaultCryptoSnapshot(),
      sourceProtocolVersion: Source[ProtocolVersion] = Source(defaultProtocolVersion),
      overrideRecipients: Option[Recipients] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {

    val cids = fullTree.tree.view.tryUnwrap.contracts.contractIds
    logger.info(s"Malicious participant $participantId submitting unassignment request for $cids")

    val rootHash = fullTree.rootHash
    val stakeholders = fullTree.stakeholders

    ResourceUtil.withResourceM(
      new SessionKeyStoreWithInMemoryCache(
        CachingConfigs.defaultSessionEncryptionKeyCacheConfig,
        timeouts,
        loggerFactory,
      )
    ) { sessionKeyStore =>
      for {
        submittingParticipantSignature <- cryptoSnapshot
          .sign(rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
          .leftMap(_.toString)
        mediatorMessage = fullTree.mediatorMessage(
          submittingParticipantSignature,
          sourceProtocolVersion,
        )
        recipientsSet <- EitherT
          .right[String](
            cryptoSnapshot.ipsSnapshot
              .activeParticipantsOfParties(stakeholders.all.toSeq)
              .map(_.values.flatten.toSet)
          )
        recipients <- EitherT.fromEither[FutureUnlessShutdown](
          overrideRecipients.orElse(Recipients.ofSet(recipientsSet)).toRight("no recipients")
        )
        viewsToKeyMap <- EncryptedViewMessageFactory
          .generateKeysFromRecipients(
            Seq(
              (
                ViewHashAndRecipients(fullTree.viewHash, recipients),
                None,
                fullTree.informees.toList,
              )
            ),
            parallel = true,
            pureCrypto,
            cryptoSnapshot,
            sessionKeyStore,
          )
          .leftMap(_.show)
        ViewKeyData(_, viewKey, viewKeyMap) = viewsToKeyMap(fullTree.viewHash)
        viewMessage <- EncryptedViewMessageFactory
          .create(UnassignmentViewType)(
            fullTree,
            (viewKey, viewKeyMap),
            cryptoSnapshot,
            sourceProtocolVersion.unwrap,
          )
          .leftMap(_.toString)
        rootHashMessage =
          RootHashMessage(
            rootHash,
            fullTree.psid,
            ViewType.UnassignmentViewType,
            cryptoSnapshot.ipsSnapshot.timestamp,
            EmptyRootHashMessagePayload,
          )
        rootHashRecipients =
          Recipients.recipientGroups(
            checked(
              NonEmptyUtil.fromUnsafe(
                recipientsSet.toSeq.map(participant =>
                  NonEmpty(Set, mediator, MemberRecipient(participant))
                )
              )
            )
          )
        messages = Seq[(ProtocolMessage, Recipients)](
          mediatorMessage -> Recipients.cc(mediator),
          viewMessage -> recipients,
          rootHashMessage -> rootHashRecipients,
        )
        batch = Batch.of(sourceProtocolVersion.unwrap, messages*)
        _ <- sendRequestBatchToSequencer(batch)
      } yield ()
    }
  }

  def submitAssignmentRequest(
      submitter: LfPartyId,
      reassignmentData: UnassignmentData,
      submittingParticipant: ParticipantId = participantId,
      mediator: MediatorGroupRecipient = defaultMediatorGroup,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi = defaultCryptoSnapshot(),
      targetProtocolVersion: Target[ProtocolVersion] = Target(defaultProtocolVersion),
      overrideRecipients: Option[Recipients] = None,
      overrideReassignmentId: Option[ReassignmentId] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val reassignmentId = overrideReassignmentId.getOrElse(reassignmentData.reassignmentId)
    logger.info(
      s"Malicious participant $participantId submitting assignment request for $reassignmentId"
    )
    val sourceSynchronizer = reassignmentData.sourcePSId
    val targetSynchronizer = reassignmentData.targetPSId

    val stakeholders = reassignmentData.contractsBatch.stakeholders

    val assignmentUuid = seedGenerator.generateUuid()
    val seed = seedGenerator.generateSaltSeed()

    val commonDataSalt = Salt.tryDeriveSalt(seed, 0, pureCrypto)
    val viewSalt = Salt.tryDeriveSalt(seed, 1, pureCrypto)

    val submitterMetadata = ReassignmentSubmitterMetadata(
      submitter,
      submittingParticipant,
      LedgerCommandId.assertFromString(UUID.randomUUID().toString),
      None,
      // Hard-coding our default user id to make sure that submissions and subscriptions use the same value
      UserId.assertFromString(LedgerApiCommands.defaultUserId),
      None,
    )

    val commonData = AssignmentCommonData
      .create(pureCrypto)(
        commonDataSalt,
        sourceSynchronizer,
        targetSynchronizer,
        mediator,
        stakeholders,
        assignmentUuid,
        submitterMetadata,
        reassigningParticipants = reassignmentData.reassigningParticipants,
        unassignmentTs = reassignmentData.unassignmentTs,
      )

    ResourceUtil.withResourceM(
      new SessionKeyStoreWithInMemoryCache(
        CachingConfigs.defaultSessionEncryptionKeyCacheConfig,
        timeouts,
        loggerFactory,
      )
    ) { sessionKeyStore =>
      for {
        view <- EitherT.fromEither[FutureUnlessShutdown](
          AssignmentView
            .create(pureCrypto)(
              viewSalt,
              reassignmentId,
              reassignmentData.contractsBatch,
              targetProtocolVersion,
            )
        )
        fullTree = FullAssignmentTree(
          AssignmentViewTree(commonData, view, targetProtocolVersion, pureCrypto)
        )

        rootHash = fullTree.rootHash
        submittingParticipantSignature <- cryptoSnapshot
          .sign(rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
          .leftMap(_.toString)
        mediatorMessage = fullTree.mediatorMessage(
          submittingParticipantSignature,
          targetProtocolVersion,
        )
        recipientsSet <- EitherT
          .right[String](
            cryptoSnapshot.ipsSnapshot
              .activeParticipantsOfParties(stakeholders.all.toSeq)
              .map(_.values.flatten.toSet)
          )
        recipients <- EitherT.fromEither[FutureUnlessShutdown](
          overrideRecipients.orElse(Recipients.ofSet(recipientsSet)).toRight("no recipients")
        )
        viewsToKeyMap <- EncryptedViewMessageFactory
          .generateKeysFromRecipients(
            Seq(
              (
                ViewHashAndRecipients(fullTree.viewHash, recipients),
                None,
                fullTree.informees.toList,
              )
            ),
            parallel = true,
            pureCrypto,
            cryptoSnapshot,
            sessionKeyStore,
          )
          .leftMap(_.show)
        ViewKeyData(_, viewKey, viewKeyMap) = viewsToKeyMap(fullTree.viewHash)
        viewMessage <- EncryptedViewMessageFactory
          .create(AssignmentViewType)(
            fullTree,
            (viewKey, viewKeyMap),
            cryptoSnapshot,
            targetProtocolVersion.unwrap,
          )
          .leftMap(_.toString)
        rootHashMessage =
          RootHashMessage(
            rootHash,
            targetSynchronizer.unwrap,
            ViewType.AssignmentViewType,
            cryptoSnapshot.ipsSnapshot.timestamp,
            EmptyRootHashMessagePayload,
          )
        rootHashRecipients =
          Recipients.recipientGroups(
            checked(
              NonEmptyUtil.fromUnsafe(
                recipientsSet.toSeq.map(participant =>
                  NonEmpty(Set, mediator, MemberRecipient(participant))
                )
              )
            )
          )
        messages = Seq[(ProtocolMessage, Recipients)](
          mediatorMessage -> Recipients.cc(mediator),
          viewMessage -> recipients,
          rootHashMessage -> rootHashRecipients,
        )
        batch = Batch.of(targetProtocolVersion.unwrap, messages*)
        _ <- sendRequestBatchToSequencer(batch)
      } yield ()
    }
  }

  def submitTopologyTransactionRequest[Op <: TopologyChangeOp, M <: TopologyMapping](
      signedTopologyTransaction: SignedTopologyTransaction[Op, M],
      psid: PhysicalSynchronizerId = defaultPSId,
      protocolVersion: ProtocolVersion = defaultProtocolVersion,
      topologyTimestamp: Option[CantonTimestamp] = None,
      recipients: Recipients = Recipients.cc(AllMembersOfSynchronizer),
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SendResult.Success] = {
    logger.info(
      s"Malicious participant $participantId submitting topology transaction $signedTopologyTransaction to $psid"
    )

    val broadcast = TopologyTransactionsBroadcast(
      psid,
      List(signedTopologyTransaction),
    )
    sendRequestBatchToSequencer(
      Batch.of(protocolVersion, broadcast -> recipients),
      topologyTimestamp,
    )
  }

  def submitTopologyTransactionBroadcasts(
      topologyBroadcasts: Seq[TopologyTransactionsBroadcast],
      psid: PhysicalSynchronizerId = defaultPSId,
      protocolVersion: ProtocolVersion = defaultProtocolVersion,
      topologyTimestamp: Option[CantonTimestamp] = None,
      recipients: Recipients = Recipients.cc(AllMembersOfSynchronizer),
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SendResult.Success] = {
    logger.info(
      s"Malicious participant $participantId submitting topology transaction broadcasts ${topologyBroadcasts
          .map(_.transactions)} to $psid"
    )
    sendRequestBatchToSequencer(
      Batch.of(protocolVersion, topologyBroadcasts.map(_ -> recipients)*),
      topologyTimestamp,
    )
  }

  def submitCommand(
      command: CommandsWithMetadata,
      transactionTreeInterceptor: GenTransactionTree => GenTransactionTree = identity,
      confirmationRequestInterceptor: TransactionConfirmationRequest => TransactionConfirmationRequest =
        identity,
      envelopeInterceptor: DefaultOpenEnvelope => DefaultOpenEnvelope = identity,
      mediator: MediatorGroupRecipient = defaultMediatorGroup,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi = defaultCryptoSnapshot(),
      maxDeduplicationDuration: Duration = Duration.ofDays(7),
      protocolVersion: ProtocolVersion = defaultProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SendResult.Success] =
    ResourceUtil.withResourceM(
      new SessionKeyStoreWithInMemoryCache(
        CachingConfigs.defaultSessionEncryptionKeyCacheConfig,
        timeouts,
        loggerFactory,
      )
    ) { sessionKeyStore =>
      for {
        transactionAndMetadata <- testSubmissionService
          .interpret(command)
          .leftMap(err => s"Unable to create transaction: $err")
          .mapK(FutureUnlessShutdown.outcomeK)
        (submittedTransaction, metadata) = transactionAndMetadata
        transactionMeta = command.transactionMeta(submittedTransaction, metadata)
        transactionMetadata = TransactionMetadata
          .fromTransactionMeta(
            metaLedgerEffectiveTime = transactionMeta.ledgerEffectiveTime,
            metaPreparationTime = transactionMeta.preparationTime,
            metaOptNodeSeeds = transactionMeta.optNodeSeeds,
          )
          .value

        wfTransaction = WellFormedTransaction.checkOrThrow(
          submittedTransaction,
          transactionMetadata,
          WellFormedTransaction.WithoutSuffixes,
        )

        transactionTree <- transactionTreeFactory
          .createTransactionTree(
            wfTransaction,
            command.submitterInfo(maxDeduplicationDuration),
            command.workflowIdO.map(WorkflowId(_)),
            mediator,
            seedGenerator.generateSaltSeed(),
            seedGenerator.generateUuid(),
            cryptoSnapshot.ipsSnapshot,
            contractOfId,
            metadata.globalKeyMapping,
            sequencerClient.generateMaxSequencingTime,
            validatePackageVettings = false,
          )
          .leftMap(err => s"Unable to create transaction tree: $err")

        modifiedTransactionTree = transactionTreeInterceptor(transactionTree)

        confirmationRequest <- confirmationRequestFactory
          .createConfirmationRequest(
            modifiedTransactionTree,
            cryptoSnapshot,
            sessionKeyStore,
            protocolVersion,
          )
          .leftMap(err => s"Unable to create confirmation request: $err")

        modifiedConfirmationRequest = confirmationRequestInterceptor(confirmationRequest)
        batch <- EitherT
          .right(modifiedConfirmationRequest.asBatch(cryptoSnapshot.ipsSnapshot))
        modifiedBatch = batch.map(envelopeInterceptor)
        sendResult <- sendRequestBatchToSequencer(modifiedBatch)
      } yield sendResult
    }
}

object MaliciousParticipantNode {
  def apply(
      participant: LocalParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
      defaultProtocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      defaultMediatorGroup: MediatorGroupRecipient = MediatorGroupRecipient(
        MediatorGroupIndex.zero
      ),
      testSubmissionServiceOverrideO: Option[TestSubmissionService] = None,
  )(implicit
      env: TestConsoleEnvironment,
      traceContext: TraceContext,
  ): MaliciousParticipantNode = {
    import env.*

    val participantNode = participant.underlying.value
    val sync = participantNode.sync
    val connectedSynchronizer = sync.readyConnectedSynchronizerById(synchronizerId.logical).value

    val contractStore = sync.participantNodePersistentState.value.contractStore

    val testSubmissionService = testSubmissionServiceOverrideO.getOrElse(
      TestSubmissionService(
        participant = participant,
        // Switch off authorization, so we can also test unauthorized commands.
        checkAuthorization = false,
        enableLfDev = true,
      )
    )
    val seedGenerator = new SeedGenerator(participantNode.cryptoPureApi)
    val contractOfId = TransactionTreeFactory.contractInstanceLookup(contractStore)
    val confirmationRequestFactory = connectedSynchronizer.requestGenerator
    val sequencerClient = connectedSynchronizer.sequencerClient

    def currentCryptoSnapshot(): SynchronizerSnapshotSyncCryptoApi = sync.syncCrypto
      .tryForSynchronizer(synchronizerId, BaseTest.defaultStaticSynchronizerParameters)
      .currentSnapshotApproximation

    new MaliciousParticipantNode(
      participant.id,
      testSubmissionService,
      seedGenerator,
      contractOfId,
      confirmationRequestFactory,
      sequencerClient,
      synchronizerId,
      defaultMediatorGroup,
      participantNode.cryptoPureApi,
      () => currentCryptoSnapshot(),
      defaultProtocolVersion,
      timeouts,
      loggerFactory,
    )
  }
}
