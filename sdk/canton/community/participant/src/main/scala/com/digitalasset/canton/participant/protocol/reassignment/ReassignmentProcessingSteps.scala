// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.{EitherT, OptionT}
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, Signature}
import com.digitalasset.canton.data.ViewType.ReassignmentViewType
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullReassignmentViewTree,
  ReassignmentRef,
  ReassignmentSubmitterMetadata,
  ViewType,
}
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.ledger.participant.state.{
  CompletionInfo,
  DomainIndex,
  RequestIndex,
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  ParsedRequest,
  PendingRequestData,
  WrapsProcessorError,
}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.{
  MalformedPayload,
  NoMediatorError,
  ProcessorError,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.EncryptedViewMessageCreationError
import com.digitalasset.canton.participant.protocol.{
  ProcessingSteps,
  ProtocolProcessor,
  SubmissionTracker,
}
import com.digitalasset.canton.participant.store.ReassignmentStore.ReassignmentStoreError
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.ConfirmationResponse.InvalidConfirmationResponse
import com.digitalasset.canton.protocol.messages.Verdict.{
  Approve,
  MediatorReject,
  ParticipantReject,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{ErrorUtil, ReassignmentTag}
import com.digitalasset.canton.{LfPartyId, RequestCounter, SequencerCounter}
import com.digitalasset.daml.lf.engine

import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future, Promise}

trait ReassignmentProcessingSteps[
    SubmissionParam,
    SubmissionResult,
    RequestViewType <: ReassignmentViewType,
    PendingReassignmentType <: PendingReassignment,
] extends ProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      RequestViewType,
      ReassignmentProcessorError,
    ]
    with NamedLogging {

  val participantId: ParticipantId

  val domainId: ReassignmentTag[DomainId]

  protected def engine: DAMLe

  protected implicit def ec: ExecutionContext

  override type SubmissionSendError = ReassignmentProcessorError

  override type PendingSubmissionId = RootHash

  override type PendingSubmissions = concurrent.Map[RootHash, PendingReassignmentSubmission]

  override type PendingSubmissionData = PendingReassignmentSubmission

  override type RequestError = ReassignmentProcessorError

  override type ResultError = ReassignmentProcessorError

  override type RejectionArgs = ReassignmentProcessingSteps.RejectionArgs[PendingReassignmentType]

  override type RequestType <: ProcessingSteps.RequestType.Reassignment
  override val requestType: RequestType

  override type FullView <: FullReassignmentViewTree
  override type ParsedRequestType = ParsedReassignmentRequest[FullView]

  override def embedNoMediatorError(error: NoMediatorError): ReassignmentProcessorError =
    GenericStepsError(error)

  override def removePendingSubmission(
      pendingSubmissions: concurrent.Map[RootHash, PendingReassignmentSubmission],
      pendingSubmissionId: RootHash,
  ): Option[PendingReassignmentSubmission] =
    pendingSubmissions.remove(pendingSubmissionId)

  override def postProcessSubmissionRejectedCommand(
      error: TransactionError,
      pendingSubmission: PendingReassignmentSubmission,
  )(implicit traceContext: TraceContext): Unit =
    pendingSubmission.reassignmentCompletion.success(error.rpcStatus())

  override def postProcessResult(
      verdict: Verdict,
      pendingSubmission: PendingReassignmentSubmission,
  )(implicit traceContext: TraceContext): Unit = {
    val status = verdict match {
      case _: Approve =>
        com.google.rpc.status.Status(com.google.rpc.Code.OK_VALUE)
      case reject: MediatorReject =>
        reject.reason
      case reasons: ParticipantReject =>
        reasons.keyEvent.reason
    }
    pendingSubmission.reassignmentCompletion.success(status)
  }

  override def authenticateInputContracts(
      parsedRequest: ParsedRequestType
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, Unit] =
    // We don't authenticate input contracts on reassignments
    EitherT.pure(())

  protected def performPendingSubmissionMapUpdate(
      pendingSubmissionMap: concurrent.Map[RootHash, PendingReassignmentSubmission],
      reassignmentRef: ReassignmentRef,
      submitterLf: LfPartyId,
      rootHash: RootHash,
  ): EitherT[Future, ReassignmentProcessorError, PendingReassignmentSubmission] = {
    val pendingSubmission = PendingReassignmentSubmission()
    val existing = pendingSubmissionMap.putIfAbsent(rootHash, pendingSubmission)
    EitherT.cond[Future](
      existing.isEmpty,
      pendingSubmission,
      DuplicateReassignmentTreeHash(
        reassignmentRef,
        submitterLf,
        rootHash,
      ): ReassignmentProcessorError,
    )
  }

  protected def decryptTree(
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(
      envelope: OpenEnvelope[EncryptedViewMessage[RequestViewType]]
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, WithRecipients[
    DecryptedView
  ]]

  override def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[RequestViewType]]]],
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, DecryptedViews] = {
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

  override def computeParsedRequest(
      rc: RequestCounter,
      ts: CantonTimestamp,
      sc: SequencerCounter,
      rootViewsWithMetadata: NonEmpty[
        Seq[(WithRecipients[FullView], Option[Signature])]
      ],
      submitterMetadataO: Option[ViewSubmitterMetadata],
      isFreshOwnTimelyRequest: Boolean,
      malformedPayloads: Seq[MalformedPayload],
      mediator: MediatorGroupRecipient,
      snapshot: DomainSnapshotSyncCryptoApi,
      domainParameters: DynamicDomainParametersWithValidity,
  )(implicit traceContext: TraceContext): Future[ParsedReassignmentRequest[FullView]] = {

    val numberOfViews = rootViewsWithMetadata.size
    if (numberOfViews > 1) {
      // The root hash check ensures that all views have the same contents.
      // The recipients check ensures that the first view has the right recipients.
      // Therefore, we can discard the remaining views.
      SyncServiceAlarm
        .Warn(
          s"Received $numberOfViews instead of 1 views in Request $ts. Discarding all but the first view."
        )
        .report()
    }

    val (WithRecipients(viewTree, recipients), signature) = rootViewsWithMetadata.head1

    Future.successful(
      ParsedReassignmentRequest(
        rc,
        ts,
        sc,
        viewTree,
        recipients,
        signature,
        submitterMetadataO,
        isFreshOwnTimelyRequest,
        isConfirmingReassigningParticipant =
          viewTree.isConfirmingReassigningParticipant(participantId),
        isObservingReassigningParticipant =
          viewTree.isObservingReassigningParticipant(participantId),
        malformedPayloads,
        mediator,
        snapshot,
        domainParameters,
      )
    )
  }

  override def constructResponsesForMalformedPayloads(
      requestId: RequestId,
      rootHash: RootHash,
      malformedPayloads: Seq[MalformedPayload],
  )(implicit traceContext: TraceContext): Seq[ConfirmationResponse] =
    // TODO(i12926) This will crash the SyncDomain
    ErrorUtil.internalError(
      new UnsupportedOperationException(
        s"Received a unassignment/assignment request with id $requestId with all payloads being malformed. Crashing..."
      )
    )

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
  ): (Option[Traced[Update]], Option[PendingSubmissionId]) = {
    val rejection = Update.CommandRejected.FinalReason(error.rpcStatus())
    val isSubmittingParticipant = submitterMetadata.submittingParticipant == participantId

    lazy val completionInfo = CompletionInfo(
      actAs = List(submitterMetadata.submitter),
      applicationId = submitterMetadata.applicationId,
      commandId = submitterMetadata.commandId,
      optDeduplicationPeriod = None,
      submissionId = None,
      messageUuid = None,
    )
    val updateO = Option.when(isSubmittingParticipant)(
      Traced(
        Update.CommandRejected(
          ts.toLf,
          completionInfo,
          rejection,
          domainId.unwrap,
          Some(
            DomainIndex.of(
              RequestIndex(
                counter = rc,
                sequencerCounter = Some(sc),
                timestamp = ts,
              )
            )
          ),
        )
      )
    )
    (updateO, rootHash.some)
  }

  override def createRejectionEvent(rejectionArgs: RejectionArgs)(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, Option[Traced[Update]]] = {

    val RejectionArgs(pendingReassignment, rejectionReason) = rejectionArgs
    val isSubmittingParticipant =
      pendingReassignment.submitterMetadata.submittingParticipant == participantId

    val completionInfoO = Option.when(isSubmittingParticipant)(
      CompletionInfo(
        actAs = List(pendingReassignment.submitterMetadata.submitter),
        applicationId = pendingReassignment.submitterMetadata.applicationId,
        commandId = pendingReassignment.submitterMetadata.commandId,
        optDeduplicationPeriod = None,
        submissionId = pendingReassignment.submitterMetadata.submissionId,
        messageUuid = None,
      )
    )

    rejectionReason.logWithContext(Map("requestId" -> pendingReassignment.requestId.toString))
    val rejection = Update.CommandRejected.FinalReason(rejectionReason.reason())
    val updateO = completionInfoO.map(info =>
      Traced(
        Update.CommandRejected(
          pendingReassignment.requestId.unwrap.toLf,
          info,
          rejection,
          domainId.unwrap,
          Some(
            DomainIndex.of(
              RequestIndex(
                counter = pendingReassignment.requestCounter,
                sequencerCounter = Some(pendingReassignment.requestSequencerCounter),
                timestamp = pendingReassignment.requestId.unwrap,
              )
            )
          ),
        )
      )
    )
    Right(updateO)
  }

  override def getSubmitterInformation(
      views: Seq[DecryptedView]
  ): (Option[ViewSubmitterMetadata], Option[SubmissionTracker.SubmissionData]) = {
    val submitterMetadataO = views.map(_.submitterMetadata).headOption
    val submissionDataForTrackerO = None // Currently not used for reassignments

    (submitterMetadataO, submissionDataForTrackerO)
  }

  case class ReassignmentsSubmission(
      override val batch: Batch[DefaultOpenEnvelope],
      override val pendingSubmissionId: PendingSubmissionId,
  ) extends UntrackedSubmission {

    override def maxSequencingTimeO: OptionT[Future, CantonTimestamp] = OptionT.none

    override def embedSubmissionError(
        err: ProtocolProcessor.SubmissionProcessingError
    ): ReassignmentProcessorError =
      GenericStepsError(err)
    override def toSubmissionError(err: ReassignmentProcessorError): ReassignmentProcessorError =
      err
  }

  override def embedRequestError(
      err: ProtocolProcessor.RequestProcessingError
  ): ReassignmentProcessorError =
    GenericStepsError(err)

  override def embedResultError(
      err: ProtocolProcessor.ResultProcessingError
  ): ReassignmentProcessorError =
    GenericStepsError(err)
}

object ReassignmentProcessingSteps {

  final case class PendingReassignmentSubmission(
      reassignmentCompletion: Promise[com.google.rpc.status.Status] =
        Promise[com.google.rpc.status.Status]()
  )

  final case class ParsedReassignmentRequest[VT <: FullReassignmentViewTree](
      override val rc: RequestCounter,
      override val requestTimestamp: CantonTimestamp,
      override val sc: SequencerCounter,
      fullViewTree: VT,
      recipients: Recipients,
      signatureO: Option[Signature],
      override val submitterMetadataO: Option[ReassignmentSubmitterMetadata],
      override val isFreshOwnTimelyRequest: Boolean,
      isConfirmingReassigningParticipant: Boolean,
      isObservingReassigningParticipant: Boolean,
      override val malformedPayloads: Seq[MalformedPayload],
      override val mediator: MediatorGroupRecipient,
      override val snapshot: DomainSnapshotSyncCryptoApi,
      override val domainParameters: DynamicDomainParametersWithValidity,
  ) extends ParsedRequest[ReassignmentSubmitterMetadata] {
    override def rootHash: RootHash = fullViewTree.rootHash
  }

  trait PendingReassignment extends PendingRequestData with Product with Serializable {
    def requestId: RequestId

    def requestCounter: RequestCounter

    def requestSequencerCounter: SequencerCounter

    def submitterMetadata: ReassignmentSubmitterMetadata

    override def isCleanReplay: Boolean = false
  }

  final case class RejectionArgs[T <: PendingReassignment](
      pendingReassignment: T,
      error: TransactionRejection,
  )

  // TODO(#18531) Check whether all the errors are needed
  trait ReassignmentProcessorError
      extends WrapsProcessorError
      with Product
      with Serializable
      with PrettyPrinting {
    override def underlyingProcessorError(): Option[ProcessorError] = None

    override protected def pretty: Pretty[ReassignmentProcessorError.this.type] =
      adHocPrettyInstance

    def message: String
  }

  final case class GenericStepsError(error: ProcessorError) extends ReassignmentProcessorError {
    override def underlyingProcessorError(): Option[ProcessorError] = Some(error)

    override def message: String = error.toString
  }

  final case class InvalidReassignmentView(reason: String) extends ReassignmentProcessorError {
    override def message: String = s"Invalid reassignment common view: $reason"
  }

  final case class UnknownDomain(domainId: DomainId, context: String)
      extends ReassignmentProcessorError {
    override def message: String = s"Unknown domain $domainId when $context"
  }

  case object ApplicationShutdown extends ReassignmentProcessorError {
    override protected def pretty: Pretty[ApplicationShutdown.type] =
      prettyOfObject[ApplicationShutdown.type]
    override def message: String = "Application is shutting down"
  }

  final case class DomainNotReady(domainId: DomainId, context: String)
      extends ReassignmentProcessorError {
    override def message: String = s"Domain $domainId is not ready when $context"
  }

  final case class ReassignmentParametersError(domainId: DomainId, context: String)
      extends ReassignmentProcessorError {
    override def message: String =
      s"Unable to compute reassignment parameters for $domainId: $context"
  }

  final case class MetadataNotFound(err: engine.Error) extends ReassignmentProcessorError {
    override def message: String = s"Contract metadata not found: ${err.message}"
  }

  final case class NoTimeProofFromDomain(domainId: DomainId, reason: String)
      extends ReassignmentProcessorError {
    override def message: String = s"Cannot fetch time proof for domain `$domainId`: $reason"
  }

  final case class NotHostedOnParticipant(
      reference: ReassignmentRef,
      party: LfPartyId,
      participantId: ParticipantId,
  ) extends ReassignmentProcessorError {

    override def message: String =
      s"For $reference: $party is not hosted on $participantId"
  }

  final case class ContractMetadataMismatch(
      reassignmentRef: ReassignmentRef,
      declaredContractMetadata: ContractMetadata,
      expectedMetadata: ContractMetadata,
  ) extends ReassignmentProcessorError {
    override def message: String = s"For reassignment `$reassignmentRef`: metadata mismatch"
  }

  final case class StakeholdersMismatch(
      reassignmentRef: ReassignmentRef,
      declaredViewStakeholders: Stakeholders,
      declaredContractStakeholders: Option[Stakeholders],
      expectedStakeholders: Either[String, Stakeholders],
  ) extends ReassignmentProcessorError {
    override def message: String = s"For reassignment `$reassignmentRef`: stakeholders mismatch"
  }

  final case class NoStakeholders private (contractId: LfContractId)
      extends ReassignmentProcessorError {
    override def message: String = s"Contract $contractId does not have any stakeholder"
  }

  object NoStakeholders {
    def logAndCreate(contract: LfContractId, logger: TracedLogger)(implicit
        tc: TraceContext
    ): NoStakeholders = {
      logger.error(
        s"Attempting reassignment for contract $contract without stakeholders. All contracts should have stakeholders."
      )
      NoStakeholders(contract)
    }
  }

  final case class ContractError(message: String) extends ReassignmentProcessorError

  final case class SubmitterMustBeStakeholder(
      reference: ReassignmentRef,
      submittingParty: LfPartyId,
      stakeholders: Set[LfPartyId],
  ) extends ReassignmentProcessorError {
    override def message: String =
      s"For $reference: submitter `$submittingParty` is not a stakeholder"
  }

  final case class ReassignmentStoreFailed(
      reassignmentId: ReassignmentId,
      error: ReassignmentStoreError,
  ) extends ReassignmentProcessorError {
    override def message: String =
      s"Cannot reassign `$reassignmentId`: internal reassignment store error"
  }

  final case class EncryptionError(
      contractId: LfContractId,
      error: EncryptedViewMessageCreationError,
  ) extends ReassignmentProcessorError {
    override def message: String = s"Cannot reassign contract `$contractId`: encryption error"
  }

  final case class DecryptionError[VT <: ViewType](
      reassignmentId: ReassignmentId,
      error: EncryptedViewMessageError,
  ) extends ReassignmentProcessorError {
    override def message: String = s"Cannot reassign `$reassignmentId`: decryption error"
  }

  final case class DuplicateReassignmentTreeHash(
      reassignmentRef: ReassignmentRef,
      submitterLf: LfPartyId,
      hash: RootHash,
  ) extends ReassignmentProcessorError {
    override def message: String = s"For reassignment $reassignmentRef: duplicatehash"
  }

  final case class FailedToCreateResponse(
      reassignmentId: ReassignmentId,
      error: InvalidConfirmationResponse,
  ) extends ReassignmentProcessorError {
    override def message: String = s"Cannot reassign `$reassignmentId`: failed to create response"
  }

  final case class FieldConversionError(
      reassignmentId: ReassignmentId,
      field: String,
      error: String,
  ) extends ReassignmentProcessorError {
    override def message: String =
      s"Cannot reassign `$reassignmentId`: invalid conversion for `$field`"

    override protected def pretty: Pretty[FieldConversionError] = prettyOfClass(
      param("field", _.field.unquoted),
      param("error", _.error.unquoted),
    )
  }

  final case class ReinterpretationAborted(reassignmentRef: ReassignmentRef, reason: String)
      extends ReassignmentProcessorError {
    override def message: String =
      s"For reassignment `$reassignmentRef`: reinterpretation aborted for reason `$reason`"
  }
}
