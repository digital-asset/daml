// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, OptionT}
import cats.syntax.bifunctor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.DecryptionError.FailedToDecrypt
import com.digitalasset.canton.crypto.SyncCryptoError.SyncCryptoDecryptionError
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, Hash, HashOps, Signature}
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex
import com.digitalasset.canton.data.{
  CantonTimestamp,
  Informee,
  ViewPosition,
  ViewTree,
  ViewTypeTest,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  PendingRequestData,
  RequestType,
  WrapsProcessorError,
}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.{
  DomainParametersError,
  NoMediatorError,
}
import com.digitalasset.canton.participant.protocol.SubmissionTracker.SubmissionData
import com.digitalasset.canton.participant.protocol.TestProcessingSteps.{
  TestPendingRequestData,
  TestPendingRequestDataType,
  TestProcessingError,
  TestProcessorError,
  TestViewTree,
  TestViewType,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessResult,
  ActivenessSet,
}
import com.digitalasset.canton.participant.store.{
  ContractLookup,
  SyncDomainEphemeralState,
  SyncDomainEphemeralStateLookup,
  TransferLookup,
}
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageError.SyncCryptoDecryptError
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{
  DynamicDomainParametersWithValidity,
  LfContractId,
  RootHash,
  ViewHash,
  v0,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  DomainId,
  MediatorRef,
  Member,
  ParticipantId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, RequestCounter, SequencerCounter}
import com.google.protobuf.ByteString

import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future}

class TestProcessingSteps(
    pendingSubmissionMap: concurrent.Map[Int, Unit],
    pendingRequestData: Option[TestPendingRequestData],
    informeesOfView: ViewHash => Set[Informee] = _ => Set.empty,
    submissionDataForTrackerO: Option[SubmissionData] = None,
)(implicit val ec: ExecutionContext)
    extends ProcessingSteps[
      Int,
      Unit,
      TestViewType,
      TransactionResultMessage,
      TestProcessingError,
    ]
    with BaseTest {
  override type SubmissionResultArgs = Unit
  override type PendingDataAndResponseArgs = Unit
  override type RejectionArgs = Unit
  override type PendingSubmissions = concurrent.Map[Int, Unit]
  override type PendingSubmissionId = Int
  override type PendingSubmissionData = Unit
  override type SubmissionSendError = TestProcessingError
  override type RequestError = TestProcessingError
  override type ResultError = TestProcessingError

  override type RequestType = TestPendingRequestDataType
  override val requestType = TestPendingRequestDataType

  override def embedRequestError(
      err: ProtocolProcessor.RequestProcessingError
  ): TestProcessingError =
    TestProcessorError(err)

  override def embedResultError(err: ProtocolProcessor.ResultProcessingError): TestProcessingError =
    TestProcessorError(err)

  override def pendingSubmissions(state: SyncDomainEphemeralState): PendingSubmissions =
    pendingSubmissionMap

  override def submissionIdOfPendingRequest(pendingData: TestPendingRequestData): Int = 0

  override def removePendingSubmission(
      pendingSubmissions: concurrent.Map[Int, Unit],
      pendingSubmissionId: Int,
  ): Option[Unit] =
    pendingSubmissions.remove(pendingSubmissionId)

  override def requestKind: String = "test"

  override def submissionDescription(param: Int): String = s"submission $param"

  override def embedNoMediatorError(error: NoMediatorError): TestProcessingError =
    TestProcessorError(error)

  override def decisionTimeFor(
      parameters: DynamicDomainParametersWithValidity,
      requestTs: CantonTimestamp,
  ): Either[TestProcessingError, CantonTimestamp] = parameters
    .decisionTimeFor(requestTs)
    .leftMap(err => TestProcessorError(DomainParametersError(parameters.domainId, err)))

  override def getSubmissionDataForTracker(
      views: Seq[DecryptedView]
  ): Option[SubmissionTracker.SubmissionData] = submissionDataForTrackerO

  override def participantResponseDeadlineFor(
      parameters: DynamicDomainParametersWithValidity,
      requestTs: CantonTimestamp,
  ): Either[TestProcessingError, CantonTimestamp] = parameters
    .participantResponseDeadlineFor(requestTs)
    .leftMap(err => TestProcessorError(DomainParametersError(parameters.domainId, err)))

  override def prepareSubmission(
      param: Int,
      mediator: MediatorRef,
      ephemeralState: SyncDomainEphemeralStateLookup,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TestProcessingError, Submission] = {
    val envelope: ProtocolMessage = mock[ProtocolMessage]
    val recipient: Member = ParticipantId("participant1")
    EitherT.rightT(new UntrackedSubmission {
      override def batch: Batch[DefaultOpenEnvelope] =
        Batch.of(testedProtocolVersion, (envelope, Recipients.cc(recipient)))
      override def pendingSubmissionId: Int = param
      override def maxSequencingTimeO: OptionT[Future, CantonTimestamp] = OptionT.none

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
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
  )(implicit traceContext: TraceContext): EitherT[Future, TestProcessingError, DecryptedViews] = {
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

  override def computeActivenessSetAndPendingContracts(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      fullViewsWithSignatures: NonEmpty[
        Seq[(WithRecipients[TestViewTree], Option[Signature])]
      ],
      malformedPayloads: Seq[ProtocolProcessor.MalformedPayload],
      snapshot: DomainSnapshotSyncCryptoApi,
      mediator: MediatorRef,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TestProcessingError, CheckActivenessAndWritePendingContracts] = {
    val res = CheckActivenessAndWritePendingContracts(ActivenessSet.empty, Seq.empty, ())
    EitherT.rightT(res)
  }

  override def constructPendingDataAndResponse(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
      transferLookup: TransferLookup,
      contractLookup: ContractLookup,
      activenessResultFuture: FutureUnlessShutdown[ActivenessResult],
      pendingCursor: Future[Unit],
      mediator: MediatorRef,
      freshOwnTimelyTx: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TestProcessingError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {
    val res = StorePendingDataAndSendResponseAndCreateTimeout(
      pendingRequestData.getOrElse(
        TestPendingRequestData(RequestCounter(0), SequencerCounter(0), Set.empty, mediator)
      ),
      Seq.empty,
      (),
    )
    EitherT.rightT(res)
  }

  def constructResponsesForMalformedPayloads(
      requestId: com.digitalasset.canton.protocol.RequestId,
      malformedPayloads: Seq[
        com.digitalasset.canton.participant.protocol.ProtocolProcessor.MalformedPayload
      ],
  )(implicit
      traceContext: com.digitalasset.canton.tracing.TraceContext
  ): Seq[com.digitalasset.canton.protocol.messages.MediatorResponse] = Seq.empty

  override def eventAndSubmissionIdForInactiveMediator(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      fullViews: NonEmpty[Seq[WithRecipients[DecryptedView]]],
      freshOwnTimelyTx: Boolean,
  )(implicit traceContext: TraceContext): (Option[TimestampedEvent], Option[PendingSubmissionId]) =
    (None, None)

  override def createRejectionEvent(rejectionArgs: Unit)(implicit
      traceContext: TraceContext
  ): Either[TestProcessingError, Option[TimestampedEvent]] =
    Right(None)

  override def getCommitSetAndContractsToBeStoredAndEvent(
      eventE: Either[
        EventWithErrors[Deliver[DefaultOpenEnvelope]],
        SignedContent[Deliver[DefaultOpenEnvelope]],
      ],
      resultE: Either[MalformedMediatorRequestResult, TransactionResultMessage],
      pendingRequestData: RequestType#PendingRequestData,
      pendingSubmissionMap: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TestProcessingError, CommitAndStoreContractsAndPublishEvent] = {
    val result = CommitAndStoreContractsAndPublishEvent(None, Set.empty, None)
    EitherT.pure[Future, TestProcessingError](result)
  }

  override def postProcessSubmissionForInactiveMediator(
      declaredMediator: MediatorRef,
      timestamp: CantonTimestamp,
      pendingSubmission: Unit,
  )(implicit traceContext: TraceContext): Unit = ()

  override def postProcessResult(verdict: Verdict, pendingSubmissionO: Unit)(implicit
      traceContext: TraceContext
  ): Unit = ()

  override def authenticateInputContracts(
      pendingDataAndResponseArgs: Unit
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TestProcessingError, Unit] =
    EitherT.rightT(())
}

object TestProcessingSteps {

  final case class TestViewTree(
      viewHash: ViewHash,
      rootHash: RootHash,
      informees: Set[Informee] = Set.empty,
      viewPosition: ViewPosition = ViewPosition(List(MerkleSeqIndex(List.empty))),
      domainId: DomainId = DefaultTestIdentities.domainId,
      mediator: MediatorRef = MediatorRef(DefaultTestIdentities.mediator),
  ) extends ViewTree
      with HasVersionedToByteString {

    def toBeSigned: Option[RootHash] = None
    override def pretty: Pretty[TestViewTree] = adHocPrettyInstance
    override def toByteString(version: ProtocolVersion): ByteString =
      throw new UnsupportedOperationException("TestViewTree cannot be serialized")
  }

  case object TestViewType extends ViewTypeTest {
    override type View = TestViewTree
    override type FullView = TestViewTree

    override def toProtoEnum: v0.ViewType =
      throw new UnsupportedOperationException("TestViewType cannot be serialized")
  }
  type TestViewType = TestViewType.type

  final case class TestPendingRequestData(
      requestCounter: RequestCounter,
      requestSequencerCounter: SequencerCounter,
      pendingContracts: Set[LfContractId],
      mediator: MediatorRef,
  ) extends PendingRequestData

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
