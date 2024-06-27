// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.RegisterError
import com.digitalasset.canton.domain.sequencing.sequencer.errors.{
  CreateSubscriptionError,
  RegisterMemberError,
  SequencerAdministrationError,
  SequencerWriteError,
}
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.TimestampSelector.TimestampSelector
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
  SequencerTrafficStatus,
}
import com.digitalasset.canton.health.admin.data.{SequencerAdminStatus, SequencerHealthStatus}
import com.digitalasset.canton.health.{AtomicHealthElement, CloseableHealthQuasiComponent}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors.TrafficControlError
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerServiceDefinition
import org.apache.pekko.Done
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Errors from pruning */
sealed trait PruningError {
  def message: String
}

sealed trait PruningSupportError extends PruningError

object PruningError {

  /** The sequencer implementation does not support pruning */
  case object NotSupported extends PruningSupportError {
    lazy val message: String = "This sequencer does not support pruning"
  }

  /** The requested timestamp would cause data for enabled members to be removed potentially permanently breaking them. */
  final case class UnsafePruningPoint(
      requestedTimestamp: CantonTimestamp,
      safeTimestamp: CantonTimestamp,
  ) extends PruningError {
    override def message: String =
      s"Could not prune at [$requestedTimestamp] as the earliest safe pruning point is [$safeTimestamp]"
  }
}

/** Interface for sequencer operations.
  * The default [[DatabaseSequencer]] implementation is backed by a database run by a single operator.
  * Other implementations support operating a Sequencer on top of third party ledgers or other infrastructure.
  */
trait Sequencer
    extends SequencerPruning
    with CloseableHealthQuasiComponent
    with AtomicHealthElement
    with HasCloseContext
    with NamedLogging {
  override val name: String = Sequencer.healthName
  override type State = SequencerHealthStatus
  override def initialHealthState: SequencerHealthStatus =
    SequencerHealthStatus(isActive = true)
  override def closingState: SequencerHealthStatus = SequencerHealthStatus.shutdownStatus

  /** True if member is registered in sequencer persistent state / storage (i.e. database).
    */
  def isRegistered(member: Member)(implicit traceContext: TraceContext): Future[Boolean]

  /** True if registered member has not been disabled.
    */
  def isEnabled(member: Member)(implicit traceContext: TraceContext): Future[Boolean]

  private[sequencing] def registerMemberInternal(member: Member, timestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): EitherT[Future, RegisterError, Unit]

  def sendAsyncSigned(signedSubmission: SignedContent[SubmissionRequest])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncError, Unit]

  def sendAsync(submission: SubmissionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncError, Unit]

  def read(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, Sequencer.EventSource]

  /** Return a snapshot state that other newly onboarded sequencers can use as an initial state
    * from which to support serving events. This state depends on the provided timestamp
    * and will contain registered members, counters per member, latest timestamp (which will be greater than
    * or equal to the provided timestamp) as well as a sequencer implementation specific piece of information
    * such that all together form the point after which the new sequencer can safely operate.
    * The provided timestamp is typically the timestamp of the requesting sequencer's private key,
    * which is the point in time where it can effectively sign events.
    */
  def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, SequencerSnapshot]

  /** Disable the provided member. Should prevent them from reading or writing in the future (although they can still be addressed).
    * Their unread data can also be pruned.
    * Effectively disables all instances of this member.
    */
  def disableMember(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerAdministrationError, Unit]

  /** The first [[com.digitalasset.canton.SequencerCounter]] that this sequencer can serve for its sequencer client
    * when the sequencer topology processor's [[com.digitalasset.canton.store.SequencedEventStore]] is empty.
    * For a sequencer bootstrapped from a [[com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot]],
    * this should be at least the [[com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot.heads]] for
    * the [[com.digitalasset.canton.topology.SequencerId]].
    * For a non-bootstrapped sequencer, this can be [[com.digitalasset.canton.GenesisSequencerCounter]].
    * This is sound as pruning ensures that we never
    */
  private[sequencing] def firstSequencerCounterServeableForSequencer: SequencerCounter

  /** Return the latest known status of the specified members, either at wall clock time of this sequencer or
    * latest known sequenced event, whichever is the most recent.
    * This method should be used for information purpose only and not to get a deterministic traffic state
    * as the state will depend on current time. To get the state at a specific timestamp, use [[getTrafficStateAt]] instead.
    * If the list is empty, return the status of all members.
    * Requested members who are not registered in the Sequencer will not be in the response.
    * Registered members with no sent or received event will return an empty status.
    */
  def trafficStatus(members: Seq[Member], selector: TimestampSelector)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerTrafficStatus]

  /** Sets the traffic purchased of a member to the new provided value.
    * This will only become effective if / when properly authorized by enough sequencers according to the
    * domain owners threshold.
    */
  def setTrafficPurchased(
      member: Member,
      serial: PositiveInt,
      totalTrafficPurchased: NonNegativeLong,
      sequencerClient: SequencerClient,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficControlError, CantonTimestamp]

  /** Return the traffic state of a member at a given timestamp.
    */
  def getTrafficStateAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[TrafficState]]

  /** Return the rate limit manager for this sequencer, if it exists.
    */
  def rateLimitManager: Option[SequencerRateLimitManager] = None

  def adminServices: Seq[ServerServiceDefinition] = Seq.empty

  /** Status relating to administrative sequencer operations.
    */
  def adminStatus: SequencerAdminStatus
}

/** Sequencer pruning interface.
  */
trait SequencerPruning {

  /** Builds a pruning scheduler once storage is available
    */
  def pruningSchedulerBuilder: Option[Storage => PruningScheduler] = None

  def pruningScheduler: Option[PruningScheduler] = None

  /** Prune as much sequencer data as safely possible without breaking operation (except for members
    * that have been previously flagged as disabled).
    * Sequencers are permitted to prune to an earlier timestamp if required to for their own consistency.
    * For example, the Database Sequencer will adjust this time to a potentially earlier point in time where
    * counter checkpoints are available for all members (who aren't being ignored).
    *
    * Implementations that support pruning also update the "oldest-response-age" metric if pruning succeeds.
    */
  def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, PruningError, String]

  /** Locate a timestamp relative to the earliest available sequencer event based on an index starting at one.
    *
    * When index == 1, indicates the progress of pruning as the timestamp of the oldest unpruned response
    * When index > 1, returns the timestamp of the index'th oldest response which is useful for pruning in batches
    * when index == batchSize.
    */
  def locatePruningTimestamp(index: PositiveInt)(implicit
      traceContext: TraceContext
  ): EitherT[Future, PruningSupportError, Option[CantonTimestamp]]

  /** Report the max-event-age metric based on the oldest event timestamp and the current clock time or
    * zero if no oldest timestamp exists (e.g. events fully pruned).
    */
  def reportMaxEventAgeMetric(
      oldestEventTimestamp: Option[CantonTimestamp]
  ): Either[PruningSupportError, Unit]

  /** Newer version of acknowledgements.
    * To be active for protocol versions >= 4.
    * The signature is checked on the server side to avoid that malicious sequencers create fake
    * acknowledgements in multi-writer architectures where writers don't fully trust each other.
    *
    * Acknowledge that a member has successfully handled all events up to and including the timestamp provided.
    * Makes earlier events for this member available for pruning.
    * The timestamp is in sequencer time and will likely correspond to an event that the client has processed however
    * this is not validated.
    * It is assumed that members in consecutive calls will never acknowledge an earlier timestamp however this is also
    * not validated (and could be invalid if the member has many subscriptions from the same or many processes).
    * It is expected that members will periodically call this endpoint with their latest clean timestamp rather than
    * calling it for every event they process. The default interval is in the range of once a minute.
    *
    * A member should only acknowledge timestamps it has actually received.
    * The behaviour of the sequencer is implementation-defined when a member acknowledges a later timestamp.
    *
    * @see com.digitalasset.canton.sequencing.client.SequencerClientConfig.acknowledgementInterval for the default interval
    */
  def acknowledgeSigned(signedAcknowledgeRequest: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit]

  /** Return a structure containing the members registered with the sequencer and the latest positions of clients
    * reading events.
    */
  def pruningStatus(implicit traceContext: TraceContext): Future[SequencerPruningStatus]
}

object Sequencer extends HasLoggerName {
  val healthName: String = "sequencer"

  /** The materialized future completes when all internal side-flows of the source have completed after the kill switch
    * was pulled. Termination of the main flow must be awaited separately.
    */
  type EventSource = Source[OrdinarySerializedEventOrError, (KillSwitch, Future[Done])]

  /** Type alias for a content that is signed by the sender (as in, whoever sent the SubmissionRequest to the sequencer).
    * Note that the sequencer itself can be the "sender": for instance when processing balance updates for traffic control,
    * the sequencer will craft a SetTrafficPurchased protocol message and sign it as the "sender".
    */
  type SenderSigned[A <: HasCryptographicEvidence] = SignedContent[A]

  /** Type alias for content that has been signed by the sequencer. The purpose of this is to identify which sequencer has processed a submission request,
    * such that after the request is ordered and processed by all sequencers, each sequencer knows which sequencer received the submission request.
    * The signature here will always be one of a sequencer.
    */
  type SequencerSigned[A <: HasCryptographicEvidence] =
    SignedContent[OrderingRequest[SenderSigned[A]]]

  /** Ordering request signed by the sequencer.
    * Outer signature is the signature of the sequencer that received the submission request.
    * Inner signature is the signature of the member from which the submission request originated.
    *
    *                            ┌─────────────────┐       ┌────────────┐
    *                            │SenderSigned     │       │Sequencer   │
    * ┌─────────────────┐        │  ┌──────────────┤       │            │
    * │Sender           │signs   │  │Submission    │sends  │            │
    * │(e.g participant)├───────►│  │Request       ├──────►│            │
    * └─────────────────┘        └──┴──────────────┘       └─────┬──────┘
    *                                                            │
    *                                                            │signs
    *                                                            ▼
    *                                                 ┌──────────────────────┐
    *                                                 │SequencerSigned       │
    *                                                 │ ┌────────────────────┤
    *                                         send to │ │SenderSigned        │
    *                                         ordering│ │ ┌──────────────────┤
    *                                        ◄────────┤ │ │Submission        │
    *                                                 │ │ │Request           │
    *                                                 └─┴─┴──────────────────┘
    */
  type SignedOrderingRequest = SequencerSigned[SubmissionRequest]

  implicit class SignedOrderingRequestOps(val value: SignedOrderingRequest) extends AnyVal {
    def signedSubmissionRequest: SignedContent[SubmissionRequest] =
      value.content.content
    def submissionRequest: SubmissionRequest = signedSubmissionRequest.content
  }

  type RegisterError = SequencerWriteError[RegisterMemberError]
}
