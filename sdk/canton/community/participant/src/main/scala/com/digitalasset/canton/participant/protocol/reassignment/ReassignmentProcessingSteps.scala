// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.crypto.{
  Signature,
  SyncCryptoError,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.ViewType.ReassignmentViewType
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullReassignmentViewTree,
  ReassignmentRef,
  ReassignmentSubmitterMetadata,
  ViewPosition,
}
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.ledger.participant.state.{CompletionInfo, SequencedUpdate, Update}
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
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.EncryptedViewMessageCreationError
import com.digitalasset.canton.participant.protocol.{
  ProcessingSteps,
  ProtocolProcessor,
  SerializableContractAuthenticator,
}
import com.digitalasset.canton.participant.store.ReassignmentStore.ReassignmentStoreError
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.Verdict.{
  Approve,
  MediatorReject,
  ParticipantReject,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, ReassignmentTag}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, RequestCounter, SequencerCounter, checked}

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

  val synchronizerId: ReassignmentTag[SynchronizerId]

  protected def engine: DAMLe

  protected def serializableContractAuthenticator: SerializableContractAuthenticator

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

  def localRejectFromActivenessCheck(
      requestId: RequestId,
      activenessResult: ActivenessResult,
      validationResult: ReassignmentValidationResult,
  ): Option[LocalRejectError]

  override def authenticateInputContracts(
      parsedRequest: ParsedRequestType
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, Unit] =
    EitherT.fromEither(
      serializableContractAuthenticator
        .authenticate(parsedRequest.fullViewTree.contract)
        .leftMap[ReassignmentProcessorError](ContractError.apply)
    )

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
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(
      envelope: OpenEnvelope[EncryptedViewMessage[RequestViewType]]
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    EncryptedViewMessageError,
    (WithRecipients[DecryptedView], Option[Signature]),
  ]

  override def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[RequestViewType]]]],
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, DecryptedViews] = {
    val result = batch.toNEF
      .parTraverse(
        decryptTree(snapshot, sessionKeyStore)(_).value
      )
      .map(DecryptedViews(_))
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
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      domainParameters: DynamicSynchronizerParametersWithValidity,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ParsedReassignmentRequest[FullView]] = {

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

    FutureUnlessShutdown.pure(
      ParsedReassignmentRequest(
        rc,
        ts,
        sc,
        viewTree,
        recipients,
        signature,
        submitterMetadataO,
        isFreshOwnTimelyRequest,
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
  ): (Option[SequencedUpdate], Option[PendingSubmissionId]) = {
    val rejection = Update.CommandRejected.FinalReason(error.rpcStatus())
    val isSubmittingParticipant = submitterMetadata.submittingParticipant == participantId

    lazy val completionInfo = CompletionInfo(
      actAs = List(submitterMetadata.submitter),
      applicationId = submitterMetadata.applicationId,
      commandId = submitterMetadata.commandId,
      optDeduplicationPeriod = None,
      submissionId = None,
    )
    val updateO = Option.when(isSubmittingParticipant)(
      Update.SequencedCommandRejected(
        completionInfo,
        rejection,
        synchronizerId.unwrap,
        rc,
        sc,
        ts,
      )
    )
    (updateO, rootHash.some)
  }

  override def createRejectionEvent(rejectionArgs: RejectionArgs)(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, Option[SequencedUpdate]] = {

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
      )
    )

    rejectionReason.logWithContext(Map("requestId" -> pendingReassignment.requestId.toString))
    val rejection = Update.CommandRejected.FinalReason(rejectionReason.reason())
    val updateO = completionInfoO.map(info =>
      Update.SequencedCommandRejected(
        info,
        rejection,
        synchronizerId.unwrap,
        pendingReassignment.requestCounter,
        pendingReassignment.requestSequencerCounter,
        pendingReassignment.requestId.unwrap,
      )
    )
    Right(updateO)
  }

  override def getSubmitterInformation(views: Seq[DecryptedView]): Option[ViewSubmitterMetadata] =
    views.map(_.submitterMetadata).headOption

  case class ReassignmentsSubmission(
      override val batch: Batch[DefaultOpenEnvelope],
      override val pendingSubmissionId: PendingSubmissionId,
  ) extends UntrackedSubmission {

    override def maxSequencingTimeO: OptionT[FutureUnlessShutdown, CantonTimestamp] = OptionT.none

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

  protected def createConfirmationResponse(
      requestId: RequestId,
      topologySnapshot: TopologySnapshot,
      protocolVersion: ProtocolVersion,
      confirmingParties: Set[LfPartyId],
      validationResult: ReassignmentValidationResult,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[ConfirmationResponse]] =
    for {
      hostedConfirmingParties <-
        if (validationResult.isReassigningParticipant)
          topologySnapshot.canConfirm(
            participantId,
            confirmingParties,
          )
        else
          FutureUnlessShutdown.pure(Set.empty[LfPartyId])

      metadataResult <-
        if (hostedConfirmingParties.nonEmpty) validationResult.metadataResultET.value
        else
          FutureUnlessShutdown.pure(Right(()))
    } yield {
      if (hostedConfirmingParties.isEmpty) None
      else {
        val activenessResult = validationResult.activenessResult
        val authenticationErrorO = validationResult.authenticationErrorO

        val authenticationRejection = authenticationErrorO.map(err =>
          LocalRejectError.MalformedRejects.MalformedRequest
            .Reject(err.message)
        )

        val modelConformanceRejection = metadataResult.swap.toSeq.map(err =>
          LocalRejectError.MalformedRejects.ModelConformance.Reject(err.toString)
        )

        val failedValidationRejection =
          validationResult.validationErrors.map(err =>
            LocalRejectError.ReassignmentRejects.ValidationFailed.Reject(err.message)
          )

        val contractRejection =
          localRejectFromActivenessCheck(requestId, activenessResult, validationResult)

        val localRejections =
          (modelConformanceRejection ++ contractRejection.toList ++ authenticationRejection.toList ++ failedValidationRejection)
            .map { err =>
              err.logWithContext()
              err.toLocalReject(protocolVersion)
            }

        val (localVerdict, parties) = localRejections
          .collectFirst[(LocalVerdict, Set[LfPartyId])] {
            case malformed: LocalReject if malformed.isMalformed => malformed -> Set.empty
            case localReject: LocalReject =>
              localReject -> hostedConfirmingParties
          }
          .getOrElse(
            LocalApprove(protocolVersion) -> hostedConfirmingParties
          )

        val confirmationResponse = checked(
          ConfirmationResponse
            .tryCreate(
              requestId,
              participantId,
              Some(ViewPosition.root),
              localVerdict,
              validationResult.rootHash,
              parties,
              synchronizerId.unwrap,
              protocolVersion,
            )
        )
        Some(confirmationResponse)
      }
    }

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
      override val malformedPayloads: Seq[MalformedPayload],
      override val mediator: MediatorGroupRecipient,
      override val snapshot: SynchronizerSnapshotSyncCryptoApi,
      override val domainParameters: DynamicSynchronizerParametersWithValidity,
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

  /** Used to convert ReassignmentValidationError to ReassignmentValidationError */
  final case class SubmissionValidationError(message: String) extends ReassignmentProcessorError

  final case class GenericStepsError(error: ProcessorError) extends ReassignmentProcessorError {
    override def underlyingProcessorError(): Option[ProcessorError] = Some(error)

    override def message: String = error.toString
  }

  final case class InvalidReassignmentView(reason: String) extends ReassignmentProcessorError {
    override def message: String = s"Invalid reassignment common view: $reason"
  }

  final case class ContractError(message: String) extends ReassignmentProcessorError

  final case class UnknownSynchronizer(synchronizerId: SynchronizerId, context: String)
      extends ReassignmentProcessorError {
    override def message: String = s"Unknown synchronizer $synchronizerId when $context"
  }

  case object ApplicationShutdown extends ReassignmentProcessorError {
    override protected def pretty: Pretty[ApplicationShutdown.type] =
      prettyOfObject[ApplicationShutdown.type]
    override def message: String = "Application is shutting down"
  }

  final case class SynchronizerNotReady(synchronizerId: SynchronizerId, context: String)
      extends ReassignmentProcessorError {
    override def message: String = s"Synchronizer $synchronizerId is not ready when $context"
  }

  final case class ReassignmentParametersError(synchronizerId: SynchronizerId, context: String)
      extends ReassignmentProcessorError {
    override def message: String =
      s"Unable to compute reassignment parameters for $synchronizerId: $context"
  }

  final case class NoTimeProofFromSynchronizer(synchronizerId: SynchronizerId, reason: String)
      extends ReassignmentProcessorError {
    override def message: String =
      s"Cannot fetch time proof for synchronizer `$synchronizerId`: $reason"
  }

  final case class ReassignmentDataNotFound(reassignmentId: ReassignmentId)
      extends ReassignmentProcessorError {
    override def message: String = s"Cannot assign `$reassignmentId`: reassignment data not found"
  }

  final case class ReassignmentSigningError(
      cause: SyncCryptoError
  ) extends ReassignmentProcessorError {
    override def message: String = show"Unable to sign reassignment request. $cause"
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

  final case class DuplicateReassignmentTreeHash(
      reassignmentRef: ReassignmentRef,
      submitterLf: LfPartyId,
      hash: RootHash,
  ) extends ReassignmentProcessorError {
    override def message: String = s"For reassignment $reassignmentRef: duplicatehash"
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
}
