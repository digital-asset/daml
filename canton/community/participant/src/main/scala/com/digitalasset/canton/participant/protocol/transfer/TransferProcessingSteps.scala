// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.lf.engine
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, Signature}
import com.digitalasset.canton.data.ViewType.TransferViewType
import com.digitalasset.canton.data.{CantonTimestamp, TransferSubmitterMetadata, ViewType}
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.ledger.participant.state.v2.CompletionInfo
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.RequestOffset
import com.digitalasset.canton.participant.protocol.ProcessingSteps.WrapsProcessorError
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.{
  MalformedPayload,
  NoMediatorError,
  ProcessorError,
}
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.EncryptedViewMessageCreationError
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
import com.digitalasset.canton.participant.protocol.{
  ProcessingSteps,
  ProtocolProcessor,
  SubmissionTracker,
}
import com.digitalasset.canton.participant.store.TransferStore.TransferStoreError
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.ConfirmationResponse.InvalidConfirmationResponse
import com.digitalasset.canton.protocol.messages.Verdict.{
  Approve,
  MediatorReject,
  ParticipantReject,
}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope, WithRecipients}
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{LfPartyId, RequestCounter, SequencerCounter}

import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future, Promise}

trait TransferProcessingSteps[
    SubmissionParam,
    SubmissionResult,
    RequestViewType <: TransferViewType,
    Result <: SignedProtocolMessageContent,
    PendingTransferType <: PendingTransfer,
] extends ProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      RequestViewType,
      Result,
      TransferProcessorError,
    ]
    with NamedLogging {

  val participantId: ParticipantId

  val domainId: TransferDomainId

  protected def engine: DAMLe

  protected implicit def ec: ExecutionContext

  override type SubmissionSendError = TransferProcessorError

  override type PendingSubmissionId = RootHash

  override type PendingSubmissions = concurrent.Map[RootHash, PendingTransferSubmission]

  override type PendingSubmissionData = PendingTransferSubmission

  override type RequestError = TransferProcessorError

  override type ResultError = TransferProcessorError

  override type RejectionArgs = TransferProcessingSteps.RejectionArgs[PendingTransferType]

  override type RequestType <: ProcessingSteps.RequestType.Transfer
  override val requestType: RequestType

  override def embedNoMediatorError(error: NoMediatorError): TransferProcessorError =
    GenericStepsError(error)

  override def removePendingSubmission(
      pendingSubmissions: concurrent.Map[RootHash, PendingTransferSubmission],
      pendingSubmissionId: RootHash,
  ): Option[PendingTransferSubmission] =
    pendingSubmissions.remove(pendingSubmissionId)

  override def postProcessSubmissionRejectedCommand(
      error: TransactionError,
      pendingSubmission: PendingTransferSubmission,
  )(implicit traceContext: TraceContext): Unit =
    pendingSubmission.transferCompletion.success(error.rpcStatus())

  override def postProcessResult(
      verdict: Verdict,
      pendingSubmission: PendingTransferSubmission,
  )(implicit traceContext: TraceContext): Unit = {
    val status = verdict match {
      case _: Approve =>
        com.google.rpc.status.Status(com.google.rpc.Code.OK_VALUE)
      case reject: MediatorReject =>
        reject.reason
      case reasons: ParticipantReject =>
        reasons.keyEvent.rpcStatus()
    }
    pendingSubmission.transferCompletion.success(status)
  }

  override def authenticateInputContracts(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Unit] = {
    // We don't authenticate input contracts on transfers
    EitherT.pure(())
  }

  protected def performPendingSubmissionMapUpdate(
      pendingSubmissionMap: concurrent.Map[RootHash, PendingTransferSubmission],
      transferId: Option[TransferId],
      submitterLf: LfPartyId,
      rootHash: RootHash,
  ): EitherT[Future, TransferProcessorError, PendingTransferSubmission] = {
    val pendingSubmission = PendingTransferSubmission()
    val existing = pendingSubmissionMap.putIfAbsent(rootHash, pendingSubmission)
    EitherT.cond[Future](
      existing.isEmpty,
      pendingSubmission,
      DuplicateTransferTreeHash(transferId, submitterLf, rootHash): TransferProcessorError,
    )
  }

  protected def decryptTree(
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
  )(
      envelope: OpenEnvelope[EncryptedViewMessage[RequestViewType]]
  )(implicit
      tc: TraceContext
  ): EitherT[Future, EncryptedViewMessageError, WithRecipients[
    DecryptedView
  ]]

  override def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[RequestViewType]]]],
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, DecryptedViews] = {
    val result = for {
      decryptedEitherList <- batch.toNEF.parTraverse(
        decryptTree(snapshot, sessionKeyStore)(_).value
      )
    } yield DecryptedViews(
      decryptedEitherList.map(_.map(decryptedView => (decryptedView, None)))
    )
    EitherT.right(result)
  }

  override def computeFullViews(
      decryptedViewsWithSignatures: Seq[(WithRecipients[DecryptedView], Option[Signature])]
  ): (Seq[(WithRecipients[FullView], Option[Signature])], Seq[ProtocolProcessor.MalformedPayload]) =
    (decryptedViewsWithSignatures, Seq.empty)

  override def constructResponsesForMalformedPayloads(
      requestId: RequestId,
      malformedPayloads: Seq[MalformedPayload],
  )(implicit traceContext: TraceContext): Seq[ConfirmationResponse] =
    // TODO(i12926) This will crash the SyncDomain
    ErrorUtil.internalError(
      new UnsupportedOperationException(
        s"Received a transfer out/in request with id $requestId with all payloads being malformed. Crashing..."
      )
    )

  protected def hostedStakeholders(
      stakeholders: List[LfPartyId],
      snapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): Future[List[LfPartyId]] =
    snapshot.hostedOn(stakeholders.toSet, participantId).map(_.keySet.toList)

  override def eventAndSubmissionIdForRejectedCommand(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      submitterMetadata: ViewSubmitterMetadata,
      rootHash: RootHash,
      freshOwnTimelyTx: Boolean,
      error: TransactionError,
  )(implicit
      traceContext: TraceContext
  ): (Option[TimestampedEvent], Option[PendingSubmissionId]) = {
    val rejection = LedgerSyncEvent.CommandRejected.FinalReason(error.rpcStatus())
    val isSubmittingParticipant = submitterMetadata.submittingParticipant == participantId

    lazy val completionInfo = CompletionInfo(
      actAs = List(submitterMetadata.submitter),
      applicationId = submitterMetadata.applicationId,
      commandId = submitterMetadata.commandId,
      optDeduplicationPeriod = None,
      submissionId = None,
      statistics = None,
    )

    val tse = Option.when(isSubmittingParticipant)(
      TimestampedEvent(
        LedgerSyncEvent
          .CommandRejected(ts.toLf, completionInfo, rejection, requestType, domainId.unwrap),
        RequestOffset(ts, rc),
        Some(sc),
      )
    )

    (tse, rootHash.some)
  }

  override def createRejectionEvent(rejectionArgs: RejectionArgs)(implicit
      traceContext: TraceContext
  ): Either[TransferProcessorError, Option[TimestampedEvent]] = {

    val RejectionArgs(pendingTransfer, rejectionReason) = rejectionArgs
    val isSubmittingParticipant =
      pendingTransfer.submitterMetadata.submittingParticipant == participantId

    val completionInfoO = Option.when(isSubmittingParticipant)(
      CompletionInfo(
        actAs = List(pendingTransfer.submitterMetadata.submitter),
        applicationId = pendingTransfer.submitterMetadata.applicationId,
        commandId = pendingTransfer.submitterMetadata.commandId,
        optDeduplicationPeriod = None,
        submissionId = pendingTransfer.submitterMetadata.submissionId,
        statistics = None,
      )
    )

    rejectionReason.logWithContext(Map("requestId" -> pendingTransfer.requestId.toString))
    val rejection = LedgerSyncEvent.CommandRejected.FinalReason(rejectionReason.rpcStatus())

    val tse = completionInfoO.map(info =>
      TimestampedEvent(
        LedgerSyncEvent
          .CommandRejected(
            pendingTransfer.requestId.unwrap.toLf,
            info,
            rejection,
            requestType,
            domainId.unwrap,
          ),
        RequestOffset(pendingTransfer.requestId.unwrap, pendingTransfer.requestCounter),
        Some(pendingTransfer.requestSequencerCounter),
      )
    )

    Right(tse)
  }

  override def decisionTimeFor(
      parameters: DynamicDomainParametersWithValidity,
      requestTs: CantonTimestamp,
  ): Either[TransferProcessorError, CantonTimestamp] =
    parameters.decisionTimeFor(requestTs).leftMap(TransferParametersError(parameters.domainId, _))

  override def getSubmitterInformation(
      views: Seq[DecryptedView]
  ): (Option[ViewSubmitterMetadata], Option[SubmissionTracker.SubmissionData]) = {
    val submitterMetadataO = views.map(_.submitterMetadata).headOption
    val submissionDataForTrackerO = None // Currently not used for transfers

    (submitterMetadataO, submissionDataForTrackerO)
  }

  override def participantResponseDeadlineFor(
      parameters: DynamicDomainParametersWithValidity,
      requestTs: CantonTimestamp,
  ): Either[TransferProcessorError, CantonTimestamp] =
    parameters
      .participantResponseDeadlineFor(requestTs)
      .leftMap(TransferParametersError(parameters.domainId, _))

  case class TransferSubmission(
      override val batch: Batch[DefaultOpenEnvelope],
      override val pendingSubmissionId: PendingSubmissionId,
  ) extends UntrackedSubmission {

    override def maxSequencingTimeO: OptionT[Future, CantonTimestamp] = OptionT.none

    override def embedSubmissionError(
        err: ProtocolProcessor.SubmissionProcessingError
    ): TransferProcessorError =
      GenericStepsError(err)
    override def toSubmissionError(err: TransferProcessorError): TransferProcessorError = err
  }

  override def embedRequestError(
      err: ProtocolProcessor.RequestProcessingError
  ): TransferProcessorError =
    GenericStepsError(err)

  override def embedResultError(
      err: ProtocolProcessor.ResultProcessingError
  ): TransferProcessorError =
    GenericStepsError(err)

}

object TransferProcessingSteps {

  final case class PendingTransferSubmission(
      transferCompletion: Promise[com.google.rpc.status.Status] =
        Promise[com.google.rpc.status.Status]()
  )

  trait PendingTransfer extends Product with Serializable {
    def requestId: RequestId

    def requestCounter: RequestCounter

    def requestSequencerCounter: SequencerCounter

    def submitterMetadata: TransferSubmitterMetadata
  }

  final case class RejectionArgs[T <: PendingTransfer](pendingTransfer: T, error: LocalReject)

  trait TransferProcessorError
      extends WrapsProcessorError
      with Product
      with Serializable
      with PrettyPrinting {
    override def underlyingProcessorError(): Option[ProcessorError] = None

    override def pretty: Pretty[TransferProcessorError.this.type] = adHocPrettyInstance

    def message: String
  }

  final case class GenericStepsError(error: ProcessorError) extends TransferProcessorError {
    override def underlyingProcessorError(): Option[ProcessorError] = Some(error)

    override def message: String = error.toString
  }

  final case class InvalidTransferCommonData(reason: String) extends TransferProcessorError {
    override def message: String = s"Invalid transfer common data: $reason"
  }

  final case class InvalidTransferView(reason: String) extends TransferProcessorError {
    override def message: String = s"Invalid transfer common view: $reason"
  }

  final case class UnknownDomain(domainId: DomainId, context: String)
      extends TransferProcessorError {
    override def message: String = s"Unknown domain $domainId when $context"
  }

  case object ApplicationShutdown extends TransferProcessorError {
    override def pretty: Pretty[ApplicationShutdown.type] = prettyOfObject[ApplicationShutdown.type]
    override def message: String = "Application is shutting down"
  }

  final case class DomainNotReady(domainId: DomainId, context: String)
      extends TransferProcessorError {
    override def message: String = s"Domain $domainId is not ready when $context"
  }

  final case class TransferParametersError(domainId: DomainId, context: String)
      extends TransferProcessorError {
    override def message: String = s"Unable to compute transfer parameters for $domainId: $context"
  }

  final case class MetadataNotFound(err: engine.Error) extends TransferProcessorError {
    override def message: String = s"Contract metadata not found: ${err.message}"
  }

  final case class CreatingTransactionIdNotFound(contractId: LfContractId)
      extends TransferProcessorError {
    override def message: String = s"Creating transaction id not found for contract `$contractId`"

  }

  final case class NoTimeProofFromDomain(domainId: DomainId, reason: String)
      extends TransferProcessorError {
    override def message: String = s"Cannot fetch time proof for domain `$domainId`: $reason"
  }

  final case class ReceivedMultipleRequests[T](transferIds: NonEmpty[Seq[T]])
      extends TransferProcessorError {
    override def message: String =
      s"Expecting a single transfer id and got several: ${transferIds.mkString(", ")}"
  }

  final case class NoTransferSubmissionPermission(
      kind: String,
      party: LfPartyId,
      participantId: ParticipantId,
  ) extends TransferProcessorError {

    override def message: String =
      s"For $kind: $party does not have submission permission on $participantId"
  }

  final case class StakeholdersMismatch(
      transferId: Option[TransferId],
      declaredViewStakeholders: Set[LfPartyId],
      declaredContractStakeholders: Option[Set[LfPartyId]],
      expectedStakeholders: Either[String, Set[LfPartyId]],
  ) extends TransferProcessorError {
    override def message: String = s"For transfer `$transferId`: stakeholders mismatch"
  }

  final case class NoStakeholders private (contractId: LfContractId)
      extends TransferProcessorError {
    override def message: String = s"Contract $contractId does not have any stakeholder"
  }

  object NoStakeholders {
    def logAndCreate(contract: LfContractId, logger: TracedLogger)(implicit
        tc: TraceContext
    ): NoStakeholders = {
      logger.error(
        s"Attempting transfer for contract $contract without stakeholders. All contracts should have stakeholders."
      )
      NoStakeholders(contract)
    }
  }

  final case class TemplateIdMismatch(
      declaredTemplateId: LfTemplateId,
      expectedTemplateId: LfTemplateId,
  ) extends TransferProcessorError {
    override def message: String =
      s"Template ID mismatch for transfer. Declared=$declaredTemplateId, expected=$expectedTemplateId`"
  }

  final case class SubmittingPartyMustBeStakeholderIn(
      transferId: TransferId,
      submittingParty: LfPartyId,
      stakeholders: Set[LfPartyId],
  ) extends TransferProcessorError {
    override def message: String =
      s"Cannot transfer-in `$transferId`: submitter `$submittingParty` is not a stakeholder"
  }

  final case class TransferStoreFailed(transferId: TransferId, error: TransferStoreError)
      extends TransferProcessorError {
    override def message: String = s"Cannot transfer `$transferId`: internal transfer store error"
  }

  final case class EncryptionError(
      contractId: LfContractId,
      error: EncryptedViewMessageCreationError,
  ) extends TransferProcessorError {
    override def message: String = s"Cannot transfer contract `$contractId`: encryption error"
  }

  final case class DecryptionError[VT <: ViewType](
      transferId: TransferId,
      error: EncryptedViewMessageError,
  ) extends TransferProcessorError {
    override def message: String = s"Cannot transfer `$transferId`: decryption error"
  }

  final case class DuplicateTransferTreeHash(
      transferId: Option[TransferId],
      submitterLf: LfPartyId,
      hash: RootHash,
  ) extends TransferProcessorError {
    private def kind = transferId.map(id => s"in: `$id`").getOrElse("out")

    override def message: String = s"Cannot transfer-$kind: duplicatehash"
  }

  final case class FailedToCreateResponse(
      transferId: TransferId,
      error: InvalidConfirmationResponse,
  ) extends TransferProcessorError {
    override def message: String = s"Cannot transfer `$transferId`: failed to create response"
  }

  final case class IncompatibleProtocolVersions(
      contractId: LfContractId,
      source: SourceProtocolVersion,
      target: TargetProtocolVersion,
  ) extends TransferProcessorError {
    override def message: String =
      s"Cannot transfer contract `$contractId`: invalid transfer from domain with protocol version $source to domain with protocol version $target"
  }

  final case class FieldConversionError(transferId: TransferId, field: String, error: String)
      extends TransferProcessorError {
    override def message: String = s"Cannot transfer `$transferId`: invalid conversion for `$field`"

    override def pretty: Pretty[FieldConversionError] = prettyOfClass(
      param("field", _.field.unquoted),
      param("error", _.error.unquoted),
    )
  }
}
