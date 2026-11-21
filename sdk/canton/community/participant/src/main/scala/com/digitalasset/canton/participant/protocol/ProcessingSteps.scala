// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, OptionT}
import cats.syntax.alternative.*
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{HashOps, Signature, SynchronizerSnapshotSyncCryptoApi}
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod, ViewType}
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.ledger.participant.state.{AcsChangeFactory, SequencedUpdate}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  InternalContractIds,
  ParsedRequest,
  WrapsProcessorError,
}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.*
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessResult,
  ActivenessSet,
  CommitSet,
}
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentProcessingSteps.PendingAssignment
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessingSteps.PendingUnassignment
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.InFlightSubmissionTrackerError
import com.digitalasset.canton.participant.protocol.submission.{
  ChangeIdHash,
  SubmissionTrackingData,
}
import com.digitalasset.canton.participant.protocol.validation.PendingTransaction
import com.digitalasset.canton.participant.store.ReassignmentLookup
import com.digitalasset.canton.participant.sync.SyncEphemeralState
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.{ConfirmationRequestSessionKeyStore, SessionKeyStore}
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LedgerSubmissionId, RequestCounter, SequencerCounter, checked}

import scala.concurrent.ExecutionContext

/** Interface for processing steps that are specific to request types (transaction / reassignment).
  * The [[ProtocolProcessor]] wires up these steps with the necessary synchronization and state
  * management, including common processing steps.
  *
  * Every phase has one main entry method (Phase i, step 1), which produces data for the
  * [[ProtocolProcessor]], The phases also have methods to be called using the results from previous
  * methods for each step.
  *
  * @tparam SubmissionParam
  *   The bundled submission parameters
  * @tparam SubmissionResult
  *   The bundled submission results
  * @tparam RequestViewType
  *   The type of view trees used by the request
  * @tparam SubmissionError
  *   The type of errors that can occur during submission processing
  */
trait ProcessingSteps[
    SubmissionParam,
    SubmissionResult,
    RequestViewType <: ViewType,
    SubmissionError <: WrapsProcessorError,
] {

  /** The type of request messages */
  type RequestBatch = RequestAndRootHashMessage[OpenEnvelope[EncryptedViewMessage[RequestViewType]]]

  /** The submission errors that can occur during sending the batch to the sequencer and updating
    * the pending submission map.
    */
  type SubmissionSendError

  /** A store of data on submissions that have been sent out, if any */
  type PendingSubmissions

  /** The data stored for submissions that have been sent out, if any. It is created by
    * [[createSubmission]] and passed to [[createSubmissionResult]]
    */
  type PendingSubmissionData <: Option[?]

  /** The type used for look-ups into the [[PendingSubmissions]] */
  type PendingSubmissionId

  /** The type of decrypted view trees */
  type DecryptedView = RequestViewType#View

  /** The type of full view trees, i.e., after decomposing light views. */
  type FullView = RequestViewType#FullView

  type ViewSubmitterMetadata = RequestViewType#ViewSubmitterMetadata

  type ViewAbsoluteLedgerEffects

  /** Type of a request that has been parsed and contains at least one well-formed view. */
  type ParsedRequestType <: ParsedRequest[ViewSubmitterMetadata]

  /** The type of data needed to create a rejection event in [[createRejectionEvent]]. Created by
    * [[constructPendingDataAndResponse]]
    */
  type RejectionArgs

  /** The type of errors that can occur during request processing */
  type RequestError <: WrapsProcessorError

  /** The type of errors that can occur during result processing */
  type ResultError <: WrapsProcessorError

  /** The type of the request (transaction, unassignment, assignment) */
  type RequestType <: ProcessingSteps.RequestType
  val requestType: RequestType

  /** Wrap an error in request processing from the generic request processor */
  def embedRequestError(err: RequestProcessingError): RequestError

  /** Wrap an error in result processing from the generic request processor */
  def embedResultError(err: ResultProcessingError): ResultError

  /** Selector to get the [[PendingSubmissions]], if any */
  def pendingSubmissions(state: SyncEphemeralState): PendingSubmissions

  /** The kind of request, used for logging and error reporting */
  def requestKind: String

  /** Extract a description for a submission, used for logging and error reporting */
  def submissionDescription(param: SubmissionParam): String

  /** Extract an optionally explicitly chosen mediator group index */
  def explicitMediatorGroup(param: SubmissionParam): Option[MediatorGroupIndex]

  /** Extract the submission ID that corresponds to a pending request, if any */
  def submissionIdOfPendingRequest(pendingData: requestType.PendingRequestData): PendingSubmissionId

  // Phase 1: Submission

  /** Phase 1, step 1:
    *
    * Creates the data that controls submission handling, and records the pending submission for
    * protocols such as reassignment that return an UntrackedSubmission.
    *
    * @param submissionParam
    *   The parameter object encapsulating the parameters of the submit method
    * @param mediator
    *   The mediator ID to use for this submission
    * @param ephemeralState
    *   Read-only access to the [[com.digitalasset.canton.participant.sync.SyncEphemeralState]]
    * @param recentSnapshot
    *   A recent snapshot of the topology state to be used for submission
    */
  def createSubmission(
      submissionParam: SubmissionParam,
      mediator: MediatorGroupRecipient,
      ephemeralState: SyncEphemeralState,
      recentSnapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SubmissionError, (Submission, PendingSubmissionData)]

  def embedNoMediatorError(error: NoMediatorError): SubmissionError

  /** Return the submitter metadata
    */
  def getSubmitterInformation(views: Seq[DecryptedView]): Option[ViewSubmitterMetadata]

  sealed trait Submission {

    /** Optional timestamp for the max sequencing time of the event.
      *
      * If possible, set it to the upper limit when the event could be successfully processed. If
      * [[scala.None]], then the sequencer client default will be used.
      */
    def maxSequencingTimeO: OptionT[FutureUnlessShutdown, CantonTimestamp]
  }

  /** Submission to be sent off without tracking the in-flight submission and without deduplication.
    */
  trait UntrackedSubmission extends Submission {

    /** The envelopes to be sent */
    def batch: Batch[DefaultOpenEnvelope]

    def pendingSubmissionId: PendingSubmissionId

    /** Wrap an error during submission from the generic request processor */
    def embedSubmissionError(err: SubmissionProcessingError): SubmissionSendError

    def toSubmissionError(err: SubmissionSendError): SubmissionError
  }

  /** Submission to be tracked in-flight and with deduplication.
    *
    * The actual batch to be sent is computed only later by [[TrackedSubmission.prepareBatch]] so
    * that tracking information (e.g., the chosen deduplication period) can be incorporated into the
    * batch.
    */
  trait TrackedSubmission extends Submission {

    /** Change id hash to be used for deduplication of requests. */
    def changeIdHash: ChangeIdHash

    /** The submission ID of the submission, optional. */
    def submissionId: Option[LedgerSubmissionId]

    /** The deduplication period for the submission as specified in the
      * [[com.digitalasset.canton.ledger.participant.state.SubmitterInfo]]
      */
    def specifiedDeduplicationPeriod: DeduplicationPeriod

    def embedSequencerRequestError(error: SequencerRequestError): SubmissionSendError

    /** The tracking data for the submission to be persisted. If the submission is not sequenced by
      * the max sequencing time, this data will be used to generate a timely rejection event via
      * [[com.digitalasset.canton.participant.protocol.submission.SubmissionTrackingData.rejectionEvent]].
      */
    def submissionTimeoutTrackingData: SubmissionTrackingData

    /** Convert a
      * [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.InFlightSubmissionTrackerError]]
      * into a `SubmissionError` to be returned by the
      * [[com.digitalasset.canton.participant.protocol.ProtocolProcessor.submit]] method.
      */
    def embedInFlightSubmissionTrackerError(error: InFlightSubmissionTrackerError): SubmissionError

    /** The submission tracking data to be used in case command deduplication failed */
    def commandDeduplicationFailure(failure: DeduplicationFailed): SubmissionTrackingData

    /** Phase 1, step 1a
      *
      * Prepare the batch of envelopes to be sent off given the
      * [[data.DeduplicationPeriod.DeduplicationOffset]] chosen by in-flight submission tracking and
      * deduplication.
      *
      * Errors will be reported asynchronously by updating the
      * [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission]] for the
      * [[changeIdHash]].
      *
      * Must not throw an exception.
      *
      * @param actualDeduplicationOffset
      *   The deduplication offset chosen by command deduplication
      */
    def prepareBatch(
        actualDeduplicationOffset: DeduplicationPeriod.DeduplicationOffset,
        maxSequencingTime: CantonTimestamp,
        sessionKeyStore: SessionKeyStore,
    ): EitherT[FutureUnlessShutdown, SubmissionTrackingData, PreparedBatch]

    /** Produce a `SubmissionError` to be returned by the
      * [[com.digitalasset.canton.participant.protocol.ProtocolProcessor.submit]] method to indicate
      * that a shutdown has happened during in-flight registration. The resulting `SubmissionError`
      * must convey neither that the submission is in-flight nor that it is not in-flight.
      */
    def shutdownDuringInFlightRegistration: SubmissionError

    /** The `SubmissionResult` to return if something went wrong after having registered the
      * submission for tracking and before the submission request was sent to a sequencer. This
      * result must not generate a completion event.
      *
      * Must not throw an exception.
      */
    def onDefinitiveFailure: SubmissionResult

    /** The submission tracking data to be used when submission fails definitely after registration
      * and before being sent to a sequencer.
      */
    def definiteFailureTrackingData(failure: UnlessShutdown[Throwable]): SubmissionTrackingData

    /** The `SubmissionResult` to return if something went wrong after having registered the
      * submission for tracking and the submission result may have been sent to a sequencer. This
      * result must not generate a completion event. It must not indicate a guaranteed submission
      * failure over the ledger API.
      *
      * Must not throw an exception.
      */
    def onPotentialFailure(maxSequencingTime: CantonTimestamp): SubmissionResult
  }

  /** The actual batch to be sent for a [[TrackedSubmission]] */
  trait PreparedBatch {

    /** The envelopes to be sent */
    def batch: Batch[DefaultOpenEnvelope]

    /** The root hash contained in the batch's root hash message */
    def rootHash: RootHash

    def pendingSubmissionId: PendingSubmissionId

    def embedSequencerRequestError(error: SequencerRequestError): SubmissionSendError

    /** The tracking data for the submission to be persisted upon a submission send error, along
      * with the timestamp at which it is supposed to be published.
      */
    def submissionErrorTrackingData(error: SubmissionSendError)(implicit
        traceContext: TraceContext
    ): SubmissionTrackingData

    /** Log the submission send error */
    def logSubmissionSendError(error: SubmissionSendError)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Unit
  }

  /** Phase 1, step 2:
    */
  def createSubmissionResult(
      deliver: Deliver[Envelope[?]],
      submissionResultArgs: PendingSubmissionData,
  ): SubmissionResult

  /** Phase 1, step 3; and Phase 7, step 1:
    *
    * Remove the pending submission from the pending submissions. Called when sending the submission
    * failed or did not lead to a result in time or a result has arrived for the request.
    */
  def removePendingSubmission(
      pendingSubmissions: PendingSubmissions,
      pendingSubmissionId: PendingSubmissionId,
  ): Option[PendingSubmissionData]

  /** Phase 1, step 3:
    *
    * Remember the [[com.digitalasset.canton.time.SynchronizerTimeTracker.TickRequest]] for the
    * decision time in the [[PendingSubmissionData]] so that the tick request can be cancelled if
    * the tick is no longer needed. Called when sending the submission succeeded.
    */
  def setDecisionTimeTickRequest(
      pendingSubmissionData: PendingSubmissionData,
      requestedTick: SynchronizerTimeTracker.TickRequest,
  ): Unit

  // Phase 3: Request processing

  /** Phase 3, step 1a:
    *
    * @param batch
    *   The batch of messages in the request to be processed
    * @param snapshot
    *   Snapshot of the topology state at the request timestamp
    * @return
    *   The decrypted views and the errors encountered during decryption
    */
  def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[RequestViewType]]]],
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RequestError, DecryptedViews]

  /** Phase 3, step 1a:
    *
    * @param views
    *   The successfully decrypted views and their signatures. Signatures are only present for
    *   top-level views (where the submitter metadata is not blinded)
    * @param decryptionErrors
    *   The decryption errors while trying to decrypt the views
    */
  case class DecryptedViews(
      views: Seq[(WithRecipients[DecryptedView], Option[Signature])],
      decryptionErrors: Seq[EncryptedViewMessageError],
  )

  object DecryptedViews {
    def apply(
        all: Seq[
          Either[EncryptedViewMessageError, (WithRecipients[DecryptedView], Option[Signature])]
        ]
    ): DecryptedViews = {
      val (errors, views) = all.separate
      DecryptedViews(views, errors)
    }
  }

  /** Phase 3, step 1b
    *
    * Extracts the ledger effects from the decrypted view and makes them absolute. When
    * absolutization fails for a view, say because an unknown contract ID versions appear in an
    * input contract value, the view should be considered malformed.
    *
    * Conflict detection needs absolutized contract IDs. As it happens in parallel to model
    * conformance, absolutization cannot be delayed until model conformance.
    *
    * @param viewsWithCorrectRootHashAndRecipientsAndSignature
    *   The list of decrypted views, as returned by [[decryptViews]], after the additional checks
    *   for the root hash and the recipients.
    */
  def absolutizeLedgerEffects(
      viewsWithCorrectRootHashAndRecipientsAndSignature: Seq[
        (WithRecipients[DecryptedView], Option[Signature])
      ]
  ): (
      Seq[(WithRecipients[DecryptedView], Option[Signature], ViewAbsoluteLedgerEffects)],
      Seq[MalformedPayload],
  )

  type FullViewAbsoluteLedgerEffects

  /** Phase 3, step 1c
    *
    * Converts the decrypted (possible light-weight) view trees to the corresponding full view
    * trees. Views that cannot be converted are mapped to [[ProtocolProcessor.MalformedPayload]]
    * errors.
    */
  def computeFullViews(
      decryptedViewsWithSignatures: Seq[
        (WithRecipients[DecryptedView], Option[Signature], ViewAbsoluteLedgerEffects)
      ]
  ): (
      Seq[(WithRecipients[FullView], Option[Signature], FullViewAbsoluteLedgerEffects)],
      Seq[MalformedPayload],
  )

  /** Phase 3, step 1d
    *
    * Create a ParsedRequest out of the data computed so far.
    */
  def computeParsedRequest(
      rc: RequestCounter,
      ts: CantonTimestamp,
      sc: SequencerCounter,
      rootViewsWithMetadata: NonEmpty[
        Seq[(WithRecipients[FullView], Option[Signature], FullViewAbsoluteLedgerEffects)]
      ],
      submitterMetadataO: Option[ViewSubmitterMetadata],
      isFreshOwnTimelyRequest: Boolean,
      malformedPayloads: Seq[MalformedPayload],
      mediator: MediatorGroupRecipient,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      synchronizerParameters: DynamicSynchronizerParametersWithValidity,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ParsedRequestType]

  /** Phase 3, step 2 (some good views) */
  def computeActivenessSet(parsedRequest: ParsedRequestType)(implicit
      traceContext: TraceContext
  ): Either[RequestError, ActivenessSet]

  /** Phase 3, step 2: Some good views, but we are rejecting (e.g. because the chosen mediator is
    * inactive or there are no valid recipients).
    *
    * @param ts
    *   The timestamp of the request
    * @param sc
    *   The [[com.digitalasset.canton.SequencerCounter]] of the request
    * @param submitterMetadata
    *   Metadata of the submitter
    * @param rootHash
    *   Root hash of the transaction
    * @param error
    *   Error to be included in the generated event
    * @param freshOwnTimelyTx
    *   The resolved status from
    *   [[com.digitalasset.canton.participant.protocol.SubmissionTracker.register]]
    *
    * @return
    *   The optional rejection event to be published in the event log, and the optional submission
    *   ID corresponding to this request
    */
  def eventAndSubmissionIdForRejectedCommand(
      ts: CantonTimestamp,
      sc: SequencerCounter,
      submitterMetadata: ViewSubmitterMetadata,
      rootHash: RootHash,
      freshOwnTimelyTx: Boolean,
      error: TransactionError,
  )(implicit
      traceContext: TraceContext
  ): (Option[SequencedUpdate], Option[PendingSubmissionId])

  /** Phase 3, step 2 (rejected submission, e.g. chosen mediator is inactive, invalid recipients)
    *
    * Called when we are rejecting the submission and [[eventAndSubmissionIdForRejectedCommand]]
    * returned a submission ID that was pending.
    *
    * @param pendingSubmission
    *   The [[PendingSubmissionData]] for the submission ID returned by
    *   [[eventAndSubmissionIdForRejectedCommand]]
    * @see
    *   com.digitalasset.canton.participant.protocol.ProcessingSteps.postProcessResult
    */
  def postProcessSubmissionRejectedCommand(
      error: TransactionError,
      pendingSubmission: PendingSubmissionData,
  )(implicit
      traceContext: TraceContext
  ): Unit

  /** Phase 3, step 3: Yields the pending data and confirmation responses for the case that at least
    * one payload is well-formed.
    *
    * @param reassignmentLookup
    *   Read-only interface of the
    *   [[com.digitalasset.canton.participant.store.memory.ReassignmentCache]]
    * @param activenessResultFuture
    *   Future of the result of the activeness check
    * @return
    *   Returns the `requestType.PendingRequestData` to be stored until Phase 7 and the responses to
    *   be sent to the mediator.
    */
  def constructPendingDataAndResponse(
      parsedRequest: ParsedRequestType,
      reassignmentLookup: ReassignmentLookup,
      activenessResultFuture: FutureUnlessShutdown[ActivenessResult],
      engineController: EngineController,
      decisionTimeTickRequest: SynchronizerTimeTracker.TickRequest,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RequestError, StorePendingDataAndSendResponseAndCreateTimeout]

  /** Phase 3: Yields the mediator responses (i.e. rejections) for the case that all payloads are
    * malformed.
    */
  def constructResponsesForMalformedPayloads(
      requestId: RequestId,
      rootHash: RootHash,
      malformedPayloads: Seq[MalformedPayload],
  )(implicit
      traceContext: TraceContext
  ): Option[ConfirmationResponses]

  /** Phase 3:
    *
    * @param pendingData
    *   The `requestType.PendingRequestData` to be stored until Phase 7
    * @param confirmationResponsesF
    *   The responses to be sent to the mediator
    * @param rejectionArgs
    *   The implementation-specific arguments needed to create a rejection event on timeout
    */
  case class StorePendingDataAndSendResponseAndCreateTimeout(
      pendingData: requestType.PendingRequestData,
      confirmationResponsesF: EitherT[FutureUnlessShutdown, RequestError, Option[
        (ConfirmationResponses, Recipients)
      ]],
      rejectionArgs: RejectionArgs,
  )

  /** Phase 3, step 4:
    *
    * @param rejectionArgs
    *   The implementation-specific information needed for the creation of the rejection event
    */
  def createRejectionEvent(rejectionArgs: RejectionArgs)(implicit
      traceContext: TraceContext
  ): Either[ResultError, Option[SequencedUpdate]]

  // Phase 7: Result processing

  /** Phase 7, step 2:
    *
    * @param event
    *   The signed [[com.digitalasset.canton.sequencing.protocol.Deliver]] event containing the
    *   confirmation result. It is ensured that the `event` contains exactly one
    *   [[com.digitalasset.canton.protocol.messages.ConfirmationResultMessage]]
    * @param verdict
    *   The verdict that is contained in the `event`
    * @param pendingRequestData
    *   The `requestType.PendingRequestData` produced in Phase 3
    * @param pendingSubmissions
    *   The data stored on submissions in the [[PendingSubmissions]]
    * @return
    *   The [[com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet]], the
    *   contracts from Phase 3 to be persisted to the contract store, and the event to be published
    */
  def getCommitSetAndContractsToBeStoredAndEventFactory(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      verdict: Verdict,
      pendingRequestData: requestType.PendingRequestData,
      pendingSubmissions: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ResultError, CommitAndStoreContractsAndPublishEvent]

  /** Phase 7, step 3:
    *
    * @param commitSet
    *   [[scala.None$]] if the request should be rejected [[scala.Some$]] a future that will produce
    *   the commit set for updating the active contract store
    * @param contractsToBeStored
    *   The contracts to be persisted to the contract store.
    * @param maybeEvent
    *   The event to be published via the
    *   [[com.digitalasset.canton.participant.event.RecordOrderPublisher]]
    */
  case class CommitAndStoreContractsAndPublishEvent(
      commitSet: Option[FutureUnlessShutdown[CommitSet]],
      contractsToBeStored: Seq[ContractInstance],
      maybeEvent: Option[AcsChangeFactory => InternalContractIds => SequencedUpdate],
  )

  /** Phase 7, step 4:
    *
    * Called after the request reached the state
    * [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Clean]] in the
    * request journal, if the participant is the submitter. Also called if a timeout occurs with
    * [[com.digitalasset.canton.protocol.messages.Verdict.MediatorReject]].
    *
    * @param verdict
    *   The verdict on the request
    */
  def postProcessResult(verdict: Verdict, pendingSubmission: PendingSubmissionData)(implicit
      traceContext: TraceContext
  ): Unit

  /** Processor specific handling of the timeout
    */
  def handleTimeout(parsedRequest: ParsedRequestType)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ResultError, Unit]
}

object ProcessingSteps {
  def getAssignmentExclusivity(
      topologySnapshot: Target[TopologySnapshot],
      ts: Target[CantonTimestamp],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Target[CantonTimestamp]] =
    for {
      synchronizerParameters <- EitherT(topologySnapshot.unwrap.findDynamicSynchronizerParameters())

      assignmentExclusivity <- EitherT
        .fromEither[FutureUnlessShutdown](
          synchronizerParameters.assignmentExclusivityLimitFor(ts.unwrap)
        )
    } yield Target(assignmentExclusivity)

  def getDecisionTime(
      topologySnapshot: TopologySnapshot,
      ts: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, CantonTimestamp] =
    for {
      synchronizerParameters <- EitherT(topologySnapshot.findDynamicSynchronizerParameters())
      decisionTime <- EitherT.fromEither[FutureUnlessShutdown](
        synchronizerParameters.decisionTimeFor(ts)
      )
    } yield decisionTime

  def constructResponsesForMalformedPayloads(
      requestId: RequestId,
      rootHash: RootHash,
      malformedPayloads: Seq[MalformedPayload],
      synchronizerId: PhysicalSynchronizerId,
      participantId: ParticipantId,
      protocolVersion: ProtocolVersion,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Option[ConfirmationResponses] = {
    val rejectError = LocalRejectError.MalformedRejects.Payloads.Reject(malformedPayloads.toString)

    val dueToTopologyChange = malformedPayloads.forall {
      case WrongRecipientsDueToTopologyChange(_) => true
      case _ => false
    }
    if (!dueToTopologyChange) rejectError.logWithContext(Map("requestId" -> requestId.toString))

    checked(
      Some(
        ConfirmationResponses
          .tryCreate(
            requestId,
            rootHash,
            synchronizerId,
            participantId,
            NonEmpty.mk(
              Seq,
              ConfirmationResponse.tryCreate(
                // We don't have to specify a viewPosition.
                // The mediator will interpret this as a rejection
                // for all views and on behalf of all declared confirming parties hosted by the participant.
                None,
                rejectError.toLocalReject(protocolVersion),
                Set.empty,
              ),
            ),
            protocolVersion,
          )
      )
    )
  }

  trait RequestType {
    type PendingRequestData <: ProcessingSteps.PendingRequestData
  }

  object RequestType {
    // Since RequestType is not sealed (extended in tests), we introduce this sealed one
    sealed trait Values extends RequestType with PrettyPrinting

    case object Transaction extends Values {
      override type PendingRequestData = PendingTransaction

      override protected def pretty: Pretty[Transaction] = prettyOfObject[Transaction]
    }
    type Transaction = Transaction.type

    sealed trait Reassignment extends Values

    case object Unassignment extends Reassignment {
      override type PendingRequestData = PendingUnassignment

      override protected def pretty: Pretty[Unassignment] = prettyOfObject[Unassignment]
    }

    type Unassignment = Unassignment.type

    case object Assignment extends Reassignment {
      override type PendingRequestData = PendingAssignment

      override protected def pretty: Pretty[Assignment] = prettyOfObject[Assignment]

    }
    type Assignment = Assignment.type
  }

  trait WrapsProcessorError {
    def underlyingProcessorError(): Option[ProcessorError]
  }

  /** Request enriched with metadata after parsing. Contains at least one wellformed view tree.
    */
  trait ParsedRequest[ViewSubmitterMetadata] {
    def rc: RequestCounter
    def requestTimestamp: CantonTimestamp
    def requestId: RequestId = RequestId(requestTimestamp)
    def sc: SequencerCounter
    def submitterMetadataO: Option[ViewSubmitterMetadata]
    def malformedPayloads: Seq[MalformedPayload]
    def snapshot: SynchronizerSnapshotSyncCryptoApi
    def mediator: MediatorGroupRecipient
    def isFreshOwnTimelyRequest: Boolean
    def synchronizerParameters: DynamicSynchronizerParametersWithValidity
    def rootHash: RootHash

    def decisionTime: CantonTimestamp = synchronizerParameters
      .decisionTimeFor(requestTimestamp)
      .valueOr(err => throw new IllegalStateException(err))
  }

  /** Data related to the request that is computed in Phase 3 and passed to Phase 7. */
  trait PendingRequestData extends Product with Serializable {
    def requestCounter: RequestCounter
    def requestSequencerCounter: SequencerCounter
    def mediator: MediatorGroupRecipient
    def locallyRejectedF: FutureUnlessShutdown[Boolean]

    /** Function to call to abort the engine execution for this request */
    def abortEngine: String => Unit

    /** Future whose completion indicates whether this request's processing terminated due to an
      * engine abort
      */
    def engineAbortStatusF: FutureUnlessShutdown[EngineAbortStatus]

    def rootHashO: Option[RootHash]

    def isCleanReplay: Boolean

    def cancelDecisionTimeTickRequest(): Unit
  }

  object PendingRequestData {
    def unapply(
        arg: PendingRequestData
    ): Some[
      (RequestCounter, SequencerCounter, MediatorGroupRecipient, FutureUnlessShutdown[Boolean])
    ] =
      Some((arg.requestCounter, arg.requestSequencerCounter, arg.mediator, arg.locallyRejectedF))
  }

  /** For better type safety, this is either a [[CleanReplayData]] or an `A`. */
  sealed trait ReplayDataOr[+A <: PendingRequestData] extends PendingRequestData {
    def toOption: Option[A]
  }

  /** This is effectively an `A`. Wrapper type for technical reasons.
    */
  final case class Wrapped[+A <: PendingRequestData](unwrap: A) extends ReplayDataOr[A] {
    override def requestCounter: RequestCounter = unwrap.requestCounter
    override def requestSequencerCounter: SequencerCounter = unwrap.requestSequencerCounter
    override def isCleanReplay: Boolean = false
    override def mediator: MediatorGroupRecipient = unwrap.mediator

    override def locallyRejectedF: FutureUnlessShutdown[Boolean] = unwrap.locallyRejectedF
    override def abortEngine: String => Unit = unwrap.abortEngine
    override val engineAbortStatusF: FutureUnlessShutdown[EngineAbortStatus] =
      unwrap.engineAbortStatusF

    override def rootHashO: Option[RootHash] = unwrap.rootHashO

    override def toOption: Option[A] = Some(unwrap)

    override def cancelDecisionTimeTickRequest(): Unit = unwrap.cancelDecisionTimeTickRequest()
  }

  /** Minimal implementation of [[PendingRequestData]] to be used in case of a clean replay. */
  final case class CleanReplayData(
      override val requestCounter: RequestCounter,
      override val requestSequencerCounter: SequencerCounter,
      override val mediator: MediatorGroupRecipient,
      override val locallyRejectedF: FutureUnlessShutdown[Boolean],
      override val abortEngine: String => Unit,
      override val engineAbortStatusF: FutureUnlessShutdown[EngineAbortStatus],
  ) extends ReplayDataOr[Nothing] {
    override def isCleanReplay: Boolean = true

    override def rootHashO: Option[RootHash] = None

    override def toOption: Option[Nothing] = None

    override def cancelDecisionTimeTickRequest(): Unit = ()
  }

  // TODO(#27996) remove this type when internal contract ids are no longer fetched from ProtocolProcessor
  type InternalContractIds = Map[LfContractId, Long]

}
