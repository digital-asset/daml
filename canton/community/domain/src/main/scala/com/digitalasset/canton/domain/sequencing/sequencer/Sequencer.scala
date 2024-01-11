// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.errors.{
  CreateSubscriptionError,
  RegisterMemberError,
  SequencerWriteError,
}
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficStatus
import com.digitalasset.canton.health.admin.data.SequencerHealthStatus
import com.digitalasset.canton.health.{AtomicHealthElement, CloseableHealthQuasiComponent}
import com.digitalasset.canton.lifecycle.HasCloseContext
import com.digitalasset.canton.logging.{HasLoggerName, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import org.apache.pekko.Done
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

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

  def isRegistered(member: Member)(implicit traceContext: TraceContext): Future[Boolean]

  def registerMember(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerWriteError[RegisterMemberError], Unit]

  /** Always returns false for Sequencer drivers that don't support ledger identity authorization. Otherwise returns
    * whether the given ledger identity is registered on the underlying ledger (and configured smart contract).
    */
  def isLedgerIdentityRegistered(identity: LedgerIdentity)(implicit
      traceContext: TraceContext
  ): Future[Boolean]

  /** Currently this method is only implemented by the enterprise-only Ethereum driver. It immediately returns a Left
    * for ledgers where it is not implemented.
    *
    * This method authorizes a [[com.digitalasset.canton.domain.sequencing.sequencer.LedgerIdentity]] on the underlying ledger.
    * In the Ethereum-backed ledger, this enables the given Ethereum account to also write to the deployed
    * `Sequencer.sol` contract. Therefore, this method needs to be called before being able to use an Ethereum sequencer
    * with a given Ethereum account.
    *
    * NB: in Ethereum, this method needs to be called by an Ethereum sequencer whose associated Ethereum account is
    * already authorized. Else the authorization itself will fail.
    * To bootstrap the authorization, the Ethereum account that deploys the `Sequencer.sol` contract is the first account
    * to be authorized.
    */
  def authorizeLedgerIdentity(identity: LedgerIdentity)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit]

  def sendAsyncSigned(signedSubmission: SignedContent[SubmissionRequest])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit]

  def sendAsync(submission: SubmissionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit]

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

  /** First check is the member is registered and if not call `registerMember` */
  def ensureRegistered(member: Member)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, SequencerWriteError[RegisterMemberError], Unit] =
    for {
      isRegistered <- EitherT.right[SequencerWriteError[RegisterMemberError]](isRegistered(member))
      _ <- EitherTUtil.ifThenET(!isRegistered)(registerMember(member))
    } yield ()

  /** Disable the provided member. Should prevent them from reading or writing in the future (although they can still be addressed).
    * Their unread data can also be pruned.
    * Effectively disables all instances of this member.
    */
  def disableMember(member: Member)(implicit traceContext: TraceContext): Future[Unit]

  /** The first [[com.digitalasset.canton.SequencerCounter]] that this sequencer can serve for its sequencer client
    * when the sequencer topology processor's [[com.digitalasset.canton.store.SequencedEventStore]] is empty.
    * For a sequencer bootstrapped from a [[com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot]],
    * this should be at least the [[com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot.heads]] for
    * the [[com.digitalasset.canton.topology.SequencerId]].
    * For a non-bootstrapped sequencer, this can be [[com.digitalasset.canton.GenesisSequencerCounter]].
    * This is sound as pruning ensures that we never
    */
  private[sequencing] def firstSequencerCounterServeableForSequencer: SequencerCounter

  /** Return the status of the specified members. If the list is empty, return the status of all members.
    */
  def trafficStatus(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): Future[SequencerTrafficStatus]

  /** Return the full traffic state of all known members.
    * This should not be exposed externally as is as it contains information not relevant to external consumers.
    * Use [[trafficStatus]] instead.
    */
  def trafficStates: Future[Map[Member, TrafficState]] = Future.successful(Map.empty)
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

  /** Acknowledge that a member has successfully handled all events up to and including the timestamp provided.
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
  def acknowledge(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

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
}
