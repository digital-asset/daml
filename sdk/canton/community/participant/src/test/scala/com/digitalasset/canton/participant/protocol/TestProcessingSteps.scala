// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, OptionT}
import cats.syntax.bifunctor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.DecryptionError.FailedToDecrypt
import com.digitalasset.canton.crypto.SyncCryptoError.SyncCryptoDecryptionError
import com.digitalasset.canton.crypto.{
  Hash,
  HashOps,
  Signature,
  SynchronizerSnapshotSyncCryptoApi,
  TestHash,
}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.ledger.participant.state.SequencedUpdate
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  ParsedRequest,
  PendingRequestData,
  RequestType,
  WrapsProcessorError,
}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.NoMediatorError
import com.digitalasset.canton.participant.protocol.TestProcessingSteps.*
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessResult,
  ActivenessSet,
}
import com.digitalasset.canton.participant.store.{
  ReassignmentLookup,
  SyncEphemeralState,
  SyncEphemeralStateLookup,
}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageError.SyncCryptoDecryptError
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParametersWithValidity,
  RootHash,
  ViewHash,
  v30,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  Member,
  ParticipantId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HasToByteString
import com.digitalasset.canton.{BaseTest, LfPartyId, RequestCounter, SequencerCounter}
import com.google.protobuf.ByteString

import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future}

class TestProcessingSteps(
    pendingSubmissionMap: concurrent.Map[Int, Unit],
    pendingRequestData: Option[TestPendingRequestData],
    informeesOfView: ViewHash => Set[LfPartyId] = _ => Set.empty,
    submissionDataForTrackerO: Option[SubmissionTrackerData] = None,
)(implicit val ec: ExecutionContext)
    extends ProcessingSteps[
      Int,
      Unit,
      TestViewType,
      TestProcessingError,
    ]
    with BaseTest {
  override type SubmissionResultArgs = Unit
  override type RejectionArgs = Unit
  override type PendingSubmissions = concurrent.Map[Int, Unit]
  override type PendingSubmissionId = Int
  override type PendingSubmissionData = Unit
  override type SubmissionSendError = TestProcessingError
  override type RequestError = TestProcessingError
  override type ResultError = TestProcessingError

  override type RequestType = TestPendingRequestDataType
  override val requestType = TestPendingRequestDataType

  override type ParsedRequestType = TestParsedRequest

  override def embedRequestError(
      err: ProtocolProcessor.RequestProcessingError
  ): TestProcessingError =
    TestProcessorError(err)

  override def embedResultError(err: ProtocolProcessor.ResultProcessingError): TestProcessingError =
    TestProcessorError(err)

  override def pendingSubmissions(state: SyncEphemeralState): PendingSubmissions =
    pendingSubmissionMap

  override def submissionIdOfPendingRequest(pendingData: TestPendingRequestData): Int = 0

  override def removePendingSubmission(
      pendingSubmissions: concurrent.Map[Int, Unit],
      pendingSubmissionId: Int,
  ): Option[Unit] =
    pendingSubmissions.remove(pendingSubmissionId)

  override def requestKind: String = "test"

  override def submissionDescription(param: Int): String = s"submission $param"

  override def explicitMediatorGroup(param: Int): Option[MediatorGroupIndex] = None

  override def embedNoMediatorError(error: NoMediatorError): TestProcessingError =
    TestProcessorError(error)

  override def getSubmitterInformation(views: Seq[DecryptedView]): Option[ViewSubmitterMetadata] =
    submissionDataForTrackerO

  override def createSubmission(
      submissionParam: Int,
      mediator: MediatorGroupRecipient,
      ephemeralState: SyncEphemeralStateLookup,
      recentSnapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TestProcessingError, Submission] = {
    val envelope: ProtocolMessage = mock[ProtocolMessage]
    val recipient: Member = ParticipantId("participant1")
    EitherT.rightT(new UntrackedSubmission {
      override def batch: Batch[DefaultOpenEnvelope] =
        Batch.of(testedProtocolVersion, (envelope, Recipients.cc(recipient)))
      override def pendingSubmissionId: Int = submissionParam
      override def maxSequencingTimeO: OptionT[FutureUnlessShutdown, CantonTimestamp] = OptionT.none

      override def embedSubmissionError(
          err: ProtocolProcessor.SubmissionProcessingError
      ): TestProcessingError =
        TestProcessorError(err)
      override def toSubmissionError(err: TestProcessingError): TestProcessingError = err
    })
  }

  override def updatePendingSubmissions(
      pendingSubmissionMap: concurrent.Map[Int, Unit],
      submissionParam: Int,
      pendingSubmissionId: Int,
  ): EitherT[Future, TestProcessingError, SubmissionResultArgs] = {
    pendingSubmissionMap.put(submissionParam, ())
    EitherT.pure(())
  }

  override def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      submissionResultArgs: SubmissionResultArgs,
  ): Unit =
    ()

  override def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TestViewType]]]],
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TestProcessingError, DecryptedViews] = {
    def treeFor(viewHash: ViewHash, hash: Hash): TestViewTree = {
      val rootHash = RootHash(hash)
      val informees = informeesOfView(viewHash)
      TestViewTree(viewHash, rootHash, informees)
    }

    val decryptedViewTrees = batch.map { envelope =>
      Hash
        .fromByteString(envelope.protocolMessage.encryptedView.viewTree.ciphertext)
        .bimap(
          err =>
            SyncCryptoDecryptError(
              SyncCryptoDecryptionError(FailedToDecrypt(err.toString))
            ),
          hash =>
            WithRecipients(treeFor(envelope.protocolMessage.viewHash, hash), envelope.recipients),
        )
    }
    EitherT.rightT(
      DecryptedViews(
        decryptedViewTrees.toList
          .map(_.map((_, None)))
      )
    )
  }

  override def computeFullViews(
      decryptedViewsWithSignatures: Seq[(WithRecipients[DecryptedView], Option[Signature])]
  ): (Seq[(WithRecipients[FullView], Option[Signature])], Seq[ProtocolProcessor.MalformedPayload]) =
    (decryptedViewsWithSignatures, Seq.empty)

  override def computeParsedRequest(
      rc: RequestCounter,
      ts: CantonTimestamp,
      sc: SequencerCounter,
      rootViewsWithMetadata: NonEmpty[Seq[(WithRecipients[FullView], Option[Signature])]],
      submitterMetadataO: Option[ViewSubmitterMetadata],
      isFreshOwnTimelyRequest: Boolean,
      malformedPayloads: Seq[ProtocolProcessor.MalformedPayload],
      mediator: MediatorGroupRecipient,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      synchronizerParameters: DynamicSynchronizerParametersWithValidity,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[TestParsedRequest] =
    FutureUnlessShutdown.pure(
      TestParsedRequest(
        rc,
        ts,
        sc,
        malformedPayloads,
        snapshot,
        mediator,
        isFreshOwnTimelyRequest,
        synchronizerParameters,
      )
    )

  override def computeActivenessSet(
      parsedRequest: ParsedRequestType
  )(implicit
      traceContext: TraceContext
  ): Either[TestProcessingError, ActivenessSet] =
    Right(ActivenessSet.empty)

  override def constructPendingDataAndResponse(
      parsedRequest: ParsedRequestType,
      reassignmentLookup: ReassignmentLookup,
      activenessResultFuture: FutureUnlessShutdown[ActivenessResult],
      engineController: EngineController,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TestProcessingError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {
    val res = StorePendingDataAndSendResponseAndCreateTimeout(
      pendingRequestData.getOrElse(
        TestPendingRequestData(
          RequestCounter(0),
          SequencerCounter(0),
          parsedRequest.mediator,
          locallyRejectedF = FutureUnlessShutdown.pure(false),
          abortEngine = _ => (),
          engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
        )
      ),
      EitherT.pure[FutureUnlessShutdown, RequestError](Seq.empty),
      (),
    )
    EitherT.rightT(res)
  }

  override def constructResponsesForMalformedPayloads(
      requestId: com.digitalasset.canton.protocol.RequestId,
      rootHash: RootHash,
      malformedPayloads: Seq[
        com.digitalasset.canton.participant.protocol.ProtocolProcessor.MalformedPayload
      ],
  )(implicit
      traceContext: com.digitalasset.canton.tracing.TraceContext
  ): Seq[com.digitalasset.canton.protocol.messages.ConfirmationResponse] = Seq.empty

  override def eventAndSubmissionIdForRejectedCommand(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      submitterMetadata: ViewSubmitterMetadata,
      rootHash: RootHash,
      freshOwnTimelyTx: Boolean,
      error: TransactionError,
  )(implicit traceContext: TraceContext): (Option[SequencedUpdate], Option[PendingSubmissionId]) =
    (None, None)

  override def createRejectionEvent(rejectionArgs: Unit)(implicit
      traceContext: TraceContext
  ): Either[TestProcessingError, Option[SequencedUpdate]] =
    Right(None)

  override def getCommitSetAndContractsToBeStoredAndEvent(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      verdict: Verdict,
      pendingRequestData: RequestType#PendingRequestData,
      pendingSubmissionMap: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TestProcessingError, CommitAndStoreContractsAndPublishEvent] = {
    val result = CommitAndStoreContractsAndPublishEvent(None, Seq.empty, None)
    EitherT.pure[FutureUnlessShutdown, TestProcessingError](result)
  }

  override def postProcessSubmissionRejectedCommand(
      error: TransactionError,
      pendingSubmission: Unit,
  )(implicit traceContext: TraceContext): Unit = ()

  override def postProcessResult(verdict: Verdict, pendingSubmissionO: Unit)(implicit
      traceContext: TraceContext
  ): Unit = ()

  override def authenticateInputContracts(
      parsedRequest: ParsedRequestType
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TestProcessingError, Unit] =
    EitherT.rightT(())

  override def handleTimeout(parsedRequest: TestParsedRequest)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TestProcessingError, Unit] = EitherT.pure(())
}

object TestProcessingSteps {

  final case class TestViewTree(
      viewHash: ViewHash,
      rootHash: RootHash,
      informees: Set[LfPartyId] = Set.empty,
      viewPosition: ViewPosition = ViewPosition(List(MerkleSeqIndex(List.empty))),
      synchronizerId: SynchronizerId = DefaultTestIdentities.synchronizerId,
      mediator: MediatorGroupRecipient = MediatorGroupRecipient(MediatorGroupIndex.zero),
  ) extends ViewTree
      with HasToByteString {

    def toBeSigned: Option[RootHash] = None
    override protected def pretty: Pretty[TestViewTree] = adHocPrettyInstance

    override def toByteString: ByteString =
      throw new UnsupportedOperationException("TestViewTree cannot be serialized")
  }

  case object TestViewType extends ViewTypeTest {
    override type View = TestViewTree
    override type FullView = TestViewTree
    override type ViewSubmitterMetadata = SubmissionTrackerData

    override def toProtoEnum: v30.ViewType =
      throw new UnsupportedOperationException("TestViewType cannot be serialized")
  }
  type TestViewType = TestViewType.type

  final case class TestParsedRequest(
      override val rc: RequestCounter,
      override val requestTimestamp: CantonTimestamp,
      override val sc: SequencerCounter,
      override val malformedPayloads: Seq[ProtocolProcessor.MalformedPayload],
      override val snapshot: SynchronizerSnapshotSyncCryptoApi,
      override val mediator: MediatorGroupRecipient,
      override val isFreshOwnTimelyRequest: Boolean,
      override val synchronizerParameters: DynamicSynchronizerParametersWithValidity,
  ) extends ParsedRequest[TestViewType.ViewSubmitterMetadata] {
    override def submitterMetadataO: None.type = None
    override def rootHash: RootHash = TestHash.dummyRootHash
  }

  final case class TestPendingRequestData(
      override val requestCounter: RequestCounter,
      override val requestSequencerCounter: SequencerCounter,
      override val mediator: MediatorGroupRecipient,
      override val locallyRejectedF: FutureUnlessShutdown[Boolean],
      override val abortEngine: String => Unit,
      override val engineAbortStatusF: FutureUnlessShutdown[EngineAbortStatus],
  ) extends PendingRequestData {

    override def rootHashO: Option[RootHash] = None

    override def isCleanReplay: Boolean = false
  }

  case object TestPendingRequestDataType extends RequestType {
    override type PendingRequestData = TestPendingRequestData
  }

  type TestPendingRequestDataType = TestPendingRequestDataType.type

  sealed trait TestProcessingError extends WrapsProcessorError

  final case class TestProcessorError(err: ProtocolProcessor.ProcessorError)
      extends TestProcessingError {
    override def underlyingProcessorError(): Option[ProtocolProcessor.ProcessorError] = Some(err)
  }

}
