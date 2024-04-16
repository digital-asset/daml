// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, OptionT}
import cats.syntax.alternative.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, HashOps, Signature}
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.WrapsProcessorError
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.*
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessResult,
  ActivenessSet,
  CommitSet,
}
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.InFlightSubmissionTrackerError
import com.digitalasset.canton.participant.protocol.submission.{
  ChangeIdHash,
  SubmissionTrackingData,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferInProcessingSteps.PendingTransferIn
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessingSteps.PendingTransferOut
import com.digitalasset.canton.participant.protocol.validation.PendingTransaction
import com.digitalasset.canton.participant.store.{
  SyncDomainEphemeralState,
  SyncDomainEphemeralStateLookup,
  TransferLookup,
}
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LedgerSubmissionId, RequestCounter, SequencerCounter}

import scala.concurrent.{ExecutionContext, Future}

/** Interface for processing steps that are specific to request types.
  * The [[ProtocolProcessor]] wires up these steps with the necessary synchronization and state management,
  * including common processing steps.
  *
  * Every phase has one main entry method (Phase i, step 1), which produces data for the [[ProtocolProcessor]],
  * The phases also have methods to be called using the results from previous methods for each step.
  *
  * @tparam SubmissionParam  The bundled submission parameters
  * @tparam SubmissionResult The bundled submission results
  * @tparam RequestViewType  The type of view trees used by the request
  * @tparam SubmissionError  The type of errors that can occur during submission processing
  */
trait ProcessingSteps[
    SubmissionParam,
    SubmissionResult,
    RequestViewType <: ViewType,
    SubmissionError <: WrapsProcessorError,
] {

  /** The type of request messages */
  type RequestBatch = RequestAndRootHashMessage[OpenEnvelope[EncryptedViewMessage[RequestViewType]]]

  /** The submission errors that can occur during sending the batch to the sequencer and updating the pending submission map. */
  type SubmissionSendError

  /** A store of data on submissions that have been sent out, if any */
  type PendingSubmissions

  /** The data stored for submissions that have been sent out, if any */
  type PendingSubmissionData

  /** The type used for look-ups into the [[PendingSubmissions]] */
  type PendingSubmissionId

  /** The type of data needed to generate the submission result in [[createSubmissionResult]].
    * The data is created by [[updatePendingSubmissions]].
    */
  type SubmissionResultArgs

  /** The type of decrypted view trees */
  type DecryptedView = RequestViewType#View

  type FullView = RequestViewType#FullView

  type ViewSubmitterMetadata = RequestViewType#ViewSubmitterMetadata

  /** The type of data needed to generate the pending data and response in [[constructPendingDataAndResponse]].
    * The data is created by [[decryptViews]]
    */
  type PendingDataAndResponseArgs

  /** The type of data needed to create a rejection event in [[createRejectionEvent]].
    * Created by [[constructPendingDataAndResponse]]
    */
  type RejectionArgs

  /** The type of errors that can occur during request processing */
  type RequestError <: WrapsProcessorError

  /** The type of errors that can occur during result processing */
  type ResultError <: WrapsProcessorError

  /** The type of the request (transaction, transfer-out, transfer-in) */
  type RequestType <: ProcessingSteps.RequestType
  val requestType: RequestType

  /** Wrap an error in request processing from the generic request processor */
  def embedRequestError(err: RequestProcessingError): RequestError

  /** Wrap an error in result processing from the generic request processor */
  def embedResultError(err: ResultProcessingError): ResultError

  /** Selector to get the [[PendingSubmissions]], if any */
  def pendingSubmissions(state: SyncDomainEphemeralState): PendingSubmissions

  /** The kind of request, used for logging and error reporting */
  def requestKind: String

  /** Extract a description for a submission, used for logging and error reporting */
  def submissionDescription(param: SubmissionParam): String

  /** Extract the submission ID that corresponds to a pending request, if any */
  def submissionIdOfPendingRequest(pendingData: requestType.PendingRequestData): PendingSubmissionId

  // Phase 1: Submission

  /** Phase 1, step 1:
    *
    * @param param          The parameter object encapsulating the parameters of the submit method
    * @param mediator       The mediator ID to use for this submission
    * @param ephemeralState Read-only access to the [[com.digitalasset.canton.participant.store.SyncDomainEphemeralState]]
    * @param recentSnapshot A recent snapshot of the topology state to be used for submission
    */
  def prepareSubmission(
      param: SubmissionParam,
      mediator: MediatorGroupRecipient,
      ephemeralState: SyncDomainEphemeralStateLookup,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SubmissionError, Submission]

  /** Convert [[com.digitalasset.canton.participant.protocol.ProtocolProcessor.NoMediatorError]] into a submission error */
  def embedNoMediatorError(error: NoMediatorError): SubmissionError

  def decisionTimeFor(
      parameters: DynamicDomainParametersWithValidity,
      requestTs: CantonTimestamp,
  ): Either[RequestError with ResultError, CantonTimestamp]

  /** Return the submitter metadata along with the submission data needed by the SubmissionTracker
    *  to decide on transaction validity
    */
  def getSubmitterInformation(
      views: Seq[DecryptedView]
  ): (Option[ViewSubmitterMetadata], Option[SubmissionTracker.SubmissionData])

  def participantResponseDeadlineFor(
      parameters: DynamicDomainParametersWithValidity,
      requestTs: CantonTimestamp,
  ): Either[RequestError with ResultError, CantonTimestamp]

  sealed trait Submission {

    /** Optional timestamp for the max sequencing time of the event.
      *
      * If possible, set it to the upper limit when the event could be successfully processed.
      * If [[scala.None]], then the sequencer client default will be used.
      */
    def maxSequencingTimeO: OptionT[Future, CantonTimestamp]
  }

  /** Submission to be sent off without tracking the in-flight submission and without deduplication. */
  trait UntrackedSubmission extends Submission {

    /** The envelopes to be sent */
    def batch: Batch[DefaultOpenEnvelope]

    def pendingSubmissionId: PendingSubmissionId

    /** Wrap an error during submission from the generic request processor */
    def embedSubmissionError(err: SubmissionProcessingError): SubmissionSendError

    /** Convert a `SubmissionSendError` to a `SubmissionError` */
    def toSubmissionError(err: SubmissionSendError): SubmissionError
  }

  /** Submission to be tracked in-flight and with deduplication.
    *
    * The actual batch to be sent is computed only later by [[TrackedSubmission.prepareBatch]]
    * so that tracking information (e.g., the chosen deduplication period) can be incorporated
    * into the batch.
    */
  trait TrackedSubmission extends Submission {

    /** Change id hash to be used for deduplication of requests. */
    def changeIdHash: ChangeIdHash

    /** The submission ID of the submission, optional. */
    def submissionId: Option[LedgerSubmissionId]

    /** The deduplication period for the submission as specified in the [[com.digitalasset.canton.ledger.participant.state.v2.SubmitterInfo]]
      */
    def specifiedDeduplicationPeriod: DeduplicationPeriod

    def embedSequencerRequestError(error: SequencerRequestError): SubmissionSendError

    /** The tracking data for the submission to be persisted.
      * If the submission is not sequenced by the max sequencing time,
      * this data will be used to generate a timely rejection event via
      * [[com.digitalasset.canton.participant.protocol.submission.SubmissionTrackingData.rejectionEvent]].
      */
    def submissionTimeoutTrackingData: SubmissionTrackingData

    /** Convert a [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.InFlightSubmissionTrackerError]]
      * into a `SubmissionError` to be returned by the
      * [[com.digitalasset.canton.participant.protocol.ProtocolProcessor.submit]] method.
      */
    def embedInFlightSubmissionTrackerError(error: InFlightSubmissionTrackerError): SubmissionError

    /** The submission tracking data to be used in case command deduplication failed */
    def commandDeduplicationFailure(failure: DeduplicationFailed): SubmissionTrackingData

    /** Phase 1, step 1a
      *
      * Prepare the batch of envelopes to be sent off
      * given the [[com.digitalasset.canton.ledger.api.DeduplicationPeriod.DeduplicationOffset]] chosen
      * by in-flight submission tracking and deduplication.
      *
      * Errors will be reported asynchronously by updating the
      * [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission]] for the [[changeIdHash]].
      *
      * Must not throw an exception.
      *
      * @param actualDeduplicationOffset The deduplication offset chosen by command deduplication
      */
    def prepareBatch(
        actualDeduplicationOffset: DeduplicationPeriod.DeduplicationOffset,
        maxSequencingTime: CantonTimestamp,
        sessionKeyStore: SessionKeyStore,
    ): EitherT[Future, SubmissionTrackingData, PreparedBatch]

    /** Produce a `SubmissionError` to be returned by the [[com.digitalasset.canton.participant.protocol.ProtocolProcessor.submit]] method
      * to indicate that a shutdown has happened during in-flight registration.
      * The resulting `SubmissionError` must convey neither that the submission is in-flight nor that it is not in-flight.
      */
    def shutdownDuringInFlightRegistration: SubmissionError

    /** The `SubmissionResult` to return if something went wrong after having registered the submission for tracking.
      * This result must not generate a completion event.
      *
      * Must not throw an exception.
      */
    def onFailure: SubmissionResult
  }

  /** The actual batch to be sent for a [[TrackedSubmission]] */
  trait PreparedBatch {

    /** The envelopes to be sent */
    def batch: Batch[DefaultOpenEnvelope]

    /** The root hash contained in the batch's root hash message */
    def rootHash: RootHash

    def pendingSubmissionId: PendingSubmissionId

    def embedSequencerRequestError(error: SequencerRequestError): SubmissionSendError

    /** The tracking data for the submission to be persisted upon a submission send error,
      * along with the timestamp at which it is supposed to be published.
      */
    def submissionErrorTrackingData(error: SubmissionSendError)(implicit
        traceContext: TraceContext
    ): SubmissionTrackingData
  }

  /** Phase 1, step 2:
    *
    * @param pendingSubmissions Stateful store to be updated with data on the pending submission
    * @param submissionParam Implementation-specific details on the submission, used for logging
    * @param pendingSubmissionId The key used for updates to the [[pendingSubmissions]]
    */
  def updatePendingSubmissions(
      pendingSubmissions: PendingSubmissions,
      submissionParam: SubmissionParam,
      pendingSubmissionId: PendingSubmissionId,
  ): EitherT[Future, SubmissionSendError, SubmissionResultArgs]

  /** Phase 1, step 3:
    */
  def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      submissionResultArgs: SubmissionResultArgs,
  ): SubmissionResult

  /** Phase 1, step 4; and Phase 7, step 1:
    *
    * Remove the pending submission from the pending submissions.
    * Called when sending the submission failed or did not lead to a result in time or a result has arrived for the request.
    */
  def removePendingSubmission(
      pendingSubmissions: PendingSubmissions,
      pendingSubmissionId: PendingSubmissionId,
  ): Option[PendingSubmissionData]

  // Phase 3: Request processing

  /** Phase 3, step 1:
    *
    * @param batch    The batch of messages in the request to be processed
    * @param snapshot Snapshot of the topology state at the request timestamp
    * @return The decrypted views and the errors encountered during decryption
    */
  def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[RequestViewType]]]],
      snapshot: DomainSnapshotSyncCryptoApi,
      sessionKeyStore: SessionKeyStore,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, RequestError, DecryptedViews]

  /** Phase 3:
    *
    * @param views The successfully decrypted views and their signatures. Signatures are only present for
    *              top-level views (where the submitter metadata is not blinded)
    * @param decryptionErrors The decryption errors while trying to decrypt the views
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

  /** Converts the decrypted (possible light-weight) view trees to the corresponding full view trees.
    * Views that cannot be converted are mapped to [[ProtocolProcessor.MalformedPayload]] errors.
    */
  def computeFullViews(
      decryptedViewsWithSignatures: Seq[(WithRecipients[DecryptedView], Option[Signature])]
  ): (Seq[(WithRecipients[FullView], Option[Signature])], Seq[MalformedPayload])

  /** Phase 3, step 2 (some good views):
    *
    * @param ts         The timestamp of the request
    * @param rc         The [[com.digitalasset.canton.RequestCounter]] of the request
    * @param sc         The [[com.digitalasset.canton.SequencerCounter]] of the request
    * @param fullViewsWithSignatures The decrypted views from step 1 with the right root hash
    *                                     and their respective signatures
    * @param malformedPayloads The decryption errors and decrypted views with a wrong root hash
    * @param snapshot Snapshot of the topology state at the request timestamp
    * @param submitterMetadataO Optional ViewSubmitterMetadata, in case fullViewsWithSignatures
    *                           might not contain unblinded ViewSubmitterMetadata
    * @return The activeness set and
    *         the contracts to store with the [[com.digitalasset.canton.participant.store.ContractStore]] in Phase 7,
    *         and the arguments for step 2.
    */
  def computeActivenessSetAndPendingContracts(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      fullViewsWithSignatures: NonEmpty[
        Seq[(WithRecipients[FullView], Option[Signature])]
      ],
      malformedPayloads: Seq[MalformedPayload],
      snapshot: DomainSnapshotSyncCryptoApi,
      mediator: MediatorGroupRecipient,
      submitterMetadataO: Option[ViewSubmitterMetadata],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, RequestError, CheckActivenessAndWritePendingContracts]

  /** Phase 3, step 2:
    * Some good views, but we are rejecting (e.g. because the chosen mediator
    * is inactive or there are no valid recipients).
    *
    * @param ts The timestamp of the request
    * @param rc The [[com.digitalasset.canton.RequestCounter]] of the request
    * @param sc The [[com.digitalasset.canton.SequencerCounter]] of the request
    * @param submitterMetadata Metadata of the submitter
    * @param rootHash Root hash of the transaction
    * @param error Error to be included in the generated event
    * @param freshOwnTimelyTx The resolved status from [[com.digitalasset.canton.participant.protocol.SubmissionTracker.register]]
    *
    * @return The optional rejection event to be published in the event log,
    *         and the optional submission ID corresponding to this request
    */
  def eventAndSubmissionIdForRejectedCommand(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      submitterMetadata: ViewSubmitterMetadata,
      rootHash: RootHash,
      freshOwnTimelyTx: Boolean,
      error: TransactionError,
  )(implicit
      traceContext: TraceContext
  ): (Option[TimestampedEvent], Option[PendingSubmissionId])

  /** Phase 3, step 2 (rejected submission, e.g. chosen mediator is inactive, invalid recipients)
    *
    * Called when we are rejecting the submission and [[eventAndSubmissionIdForRejectedCommand]]
    * returned a submission ID that was pending.
    *
    * @param pendingSubmission The [[PendingSubmissionData]] for the submission ID returned by
    *                          [[eventAndSubmissionIdForRejectedCommand]]
    * @see com.digitalasset.canton.participant.protocol.ProcessingSteps.postProcessResult
    */
  def postProcessSubmissionRejectedCommand(
      error: TransactionError,
      pendingSubmission: PendingSubmissionData,
  )(implicit
      traceContext: TraceContext
  ): Unit

  /** Phase 3
    *
    * @param activenessSet              The activeness set for the activeness check
    * @param pendingDataAndResponseArgs The implementation-specific arguments needed to create the pending data and response
    */
  case class CheckActivenessAndWritePendingContracts(
      activenessSet: ActivenessSet,
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
  )

  def authenticateInputContracts(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, RequestError, Unit]

  /** Phase 3, step 3:
    * Yields the pending data and confirmation responses for the case that at least one payload is well-formed.
    *
    * @param pendingDataAndResponseArgs Implementation-specific data passed from [[decryptViews]]
    * @param transferLookup             Read-only interface of the [[com.digitalasset.canton.participant.store.memory.TransferCache]]
    * @param activenessResultFuture     Future of the result of the activeness check<
    * @param mediator                   The mediator that handles this request
    * @return Returns the `requestType.PendingRequestData` to be stored until Phase 7 and the responses to be sent to the mediator.
    */
  def constructPendingDataAndResponse(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
      transferLookup: TransferLookup,
      activenessResultFuture: FutureUnlessShutdown[ActivenessResult],
      mediator: MediatorGroupRecipient,
      freshOwnTimelyTx: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RequestError, StorePendingDataAndSendResponseAndCreateTimeout]

  /** Phase 3:
    * Yields the mediator responses (i.e. rejections) for the case that all payloads are malformed.
    */
  def constructResponsesForMalformedPayloads(
      requestId: RequestId,
      rootHash: RootHash,
      malformedPayloads: Seq[MalformedPayload],
  )(implicit
      traceContext: TraceContext
  ): Seq[ConfirmationResponse]

  /** Phase 3:
    *
    * @param pendingData   The `requestType.PendingRequestData` to be stored until Phase 7
    * @param confirmationResponses     The responses to be sent to the mediator
    * @param rejectionArgs The implementation-specific arguments needed to create a rejection event on timeout
    */
  case class StorePendingDataAndSendResponseAndCreateTimeout(
      pendingData: requestType.PendingRequestData,
      confirmationResponses: Seq[(ConfirmationResponse, Recipients)],
      rejectionArgs: RejectionArgs,
  )

  /** Phase 3, step 4:
    *
    * @param rejectionArgs The implementation-specific information needed for the creation of the rejection event
    */
  def createRejectionEvent(rejectionArgs: RejectionArgs)(implicit
      traceContext: TraceContext
  ): Either[ResultError, Option[TimestampedEvent]]

  // Phase 7: Result processing

  /** Phase 7, step 2:
    *
    * @param event             The signed [[com.digitalasset.canton.sequencing.protocol.Deliver]] event containing the confirmation result.
    *                           It is ensured that the `event` contains exactly one [[com.digitalasset.canton.protocol.messages.ConfirmationResultMessage]]
    * @param result            The unpacked confirmation result that is contained in the `event`
    * @param pendingRequestData The `requestType.PendingRequestData` produced in Phase 3
    * @param pendingSubmissions The data stored on submissions in the [[PendingSubmissions]]
    * @return The [[com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet]],
    *         the contracts from Phase 3 to be persisted to the contract store,
    *         and the event to be published
    */
  def getCommitSetAndContractsToBeStoredAndEvent(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]],
      verdict: Verdict,
      pendingRequestData: requestType.PendingRequestData,
      pendingSubmissions: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ResultError, CommitAndStoreContractsAndPublishEvent]

  /** Phase 7, step 3:
    *
    * @param commitSet           [[scala.None$]] if the request should be rejected
    *                            [[scala.Some$]] a future that will produce the commit set for updating the active contract store
    * @param contractsToBeStored The contracts to be persisted to the contract store.
    *                            Must be a subset of the contracts produced in Phase 3, step 2 in [[CheckActivenessAndWritePendingContracts]].
    * @param maybeEvent          The event to be published via the [[com.digitalasset.canton.participant.event.RecordOrderPublisher]]
    */
  case class CommitAndStoreContractsAndPublishEvent(
      commitSet: Option[Future[CommitSet]],
      contractsToBeStored: Seq[WithTransactionId[SerializableContract]],
      maybeEvent: Option[TimestampedEvent],
  )

  /** Phase 7, step 4:
    *
    * Called after the request reached the state [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Clean]]
    * in the request journal, if the participant is the submitter.
    * Also called if a timeout occurs with [[com.digitalasset.canton.protocol.messages.Verdict.MediatorReject]].
    *
    * @param verdict The verdict on the request
    */
  def postProcessResult(verdict: Verdict, pendingSubmission: PendingSubmissionData)(implicit
      traceContext: TraceContext
  ): Unit

}

object ProcessingSteps {
  def getTransferInExclusivity(
      topologySnapshot: TopologySnapshot,
      ts: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, CantonTimestamp] =
    for {
      domainParameters <- EitherT(topologySnapshot.findDynamicDomainParameters())

      transferInExclusivity <- EitherT
        .fromEither[Future](domainParameters.transferExclusivityLimitFor(ts))
    } yield transferInExclusivity

  def getDecisionTime(
      topologySnapshot: TopologySnapshot,
      ts: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, CantonTimestamp] =
    for {
      domainParameters <- EitherT(topologySnapshot.findDynamicDomainParameters())
      decisionTime <- EitherT.fromEither[Future](domainParameters.decisionTimeFor(ts))
    } yield decisionTime

  trait RequestType {
    type PendingRequestData <: ProcessingSteps.PendingRequestData
  }

  object RequestType {
    // Since RequestType is not sealed (extended in tests), we introduce this sealed one
    sealed trait Values extends RequestType with PrettyPrinting

    case object Transaction extends Values {
      override type PendingRequestData = PendingTransaction
      override def pretty: Pretty[Transaction] = prettyOfObject[Transaction]
    }
    type Transaction = Transaction.type

    sealed trait Transfer extends Values

    case object TransferOut extends Transfer {
      override type PendingRequestData = PendingTransferOut
      override def pretty: Pretty[TransferOut] = prettyOfObject[TransferOut]
    }

    type TransferOut = TransferOut.type

    case object TransferIn extends Transfer {
      override type PendingRequestData = PendingTransferIn
      override def pretty: Pretty[TransferIn] = prettyOfObject[TransferIn]

    }
    type TransferIn = TransferIn.type
  }

  trait WrapsProcessorError {
    def underlyingProcessorError(): Option[ProcessorError]
  }

  /** Generic parts that must be passed from Phase 3 to Phase 7. */
  trait PendingRequestData {
    def requestCounter: RequestCounter
    def requestSequencerCounter: SequencerCounter
    def mediator: MediatorGroupRecipient
    def locallyRejected: Boolean

    def rootHashO: Option[RootHash]
  }

  object PendingRequestData {
    def unapply(
        arg: PendingRequestData
    ): Some[(RequestCounter, SequencerCounter, MediatorGroupRecipient, Boolean)] = {
      Some((arg.requestCounter, arg.requestSequencerCounter, arg.mediator, arg.locallyRejected))
    }
  }
}
