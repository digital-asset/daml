// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.integrations.state

import cats.data.EitherT
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.integrations.state.SequencerStateManagerStore.PruningResult
import com.digitalasset.canton.domain.sequencing.integrations.state.statemanager.MemberCounters
import com.digitalasset.canton.domain.sequencing.sequencer.store.SaveLowerBoundError
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregationUpdates,
  InternalSequencerPruningStatus,
}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Backing store for the [[com.digitalasset.canton.domain.block.BlockSequencerStateManager]] used for sequencer integrations to persist some sequencer
  * data into a database.
  */
trait SequencerStateManagerStore {

  /** Rehydrate the sequencer [[EphemeralState]] from the backing persisted store
    *
    * @param timestamp The timestamp for which the [[EphemeralState]] is computed
    */
  def readAtBlockTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[EphemeralState]

  /** Extract a range of events for a member.
    * It is expected that the sequencer will validate requests against its current state so read requests
    * for unregistered members or counter ranges that do not exist should just return an empty source.
    *
    * @throws java.lang.IllegalArgumentException if startInclusive is not less than endExclusive
    */
  def readRange(member: Member, startInclusive: SequencerCounter, endExclusive: SequencerCounter)(
      implicit traceContext: TraceContext
  ): Source[OrdinarySerializedEvent, NotUsed]

  /** Adds a new member to the sequencer.
    * Callers are expected to ensure that the member is not already registered.
    */
  def addMember(member: Member, addedAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Add a new events to the sequencer store.
    * Callers must ensure that all members receiving a counter update have been registered and that the counter values are correct.
    * Callers must also ensure that the timestamp is correct and later than all prior events as this is also not validated by all store implementations.
    * Counter updates for invalid counter values (<0) will cause a [[java.lang.IllegalArgumentException]] to be throw.
    * Implementations may throw a [[java.lang.IllegalArgumentException]] if a counter update is incorrect (not following the current head),
    * but may not if this is not possible to efficiently do (like with database persistence that would require executing a query to check).
    * Implementations should ensure that all events are written atomically (or none written if a failure is hit).
    */
  def addEvents(
      events: Map[Member, OrdinarySerializedEvent],
      trafficSate: Map[Member, TrafficState],
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Write an acknowledgement that member has processed earlier timestamps.
    * Only the latest timestamp needs to be stored. Earlier timestamps can be overwritten.
    * Acknowledgements of earlier timestamps should be ignored.
    */
  def acknowledge(
      member: Member,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit]

  /** Return the latest acknowledgements for all members */
  @VisibleForTesting
  protected[state] def latestAcknowledgements()(implicit
      traceContext: TraceContext
  ): Future[Map[Member, CantonTimestamp]]

  /** Fetch the lower bound of events that can be read. Returns `None` if all events can be read. */
  def fetchLowerBound()(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]]

  /** Save an updated lower bound of events that can be read along with the optional timestamp
    * of the initial onboarding topology snapshot.
    *
    * The lower bound of events ts must be equal or greater than any prior set lower bound.
    * The value of maybeOnboardingTopologyTimestamp is only set if nonEmpty and only on the
    * initial call.
    */
  def saveLowerBound(
      ts: CantonTimestamp,
      maybeOnboardingTopologyTimestamp: Option[CantonTimestamp] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SaveLowerBoundError, Unit]

  /** Prevents member from sending and reading from the sequencer, and allows unread data for this member to be pruned.
    * It however won't stop any sends addressed to this member.
    */
  def disableMember(member: Member)(implicit traceContext: TraceContext): Future[Unit]

  /** Check whether the member is enabled.
    * Currently used when receiving a request for reading.
    */
  def isEnabled(member: Member)(implicit
      traceContext: TraceContext
  ): Future[Boolean]

  /** Build a status object representing the current state of the sequencer. */
  def status()(implicit traceContext: TraceContext): Future[InternalSequencerPruningStatus]

  def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[PruningResult]

  /** Updates the in-flight aggregations for the given aggregation IDs.
    * Only adds or updates aggregations, but never removes them.
    *
    * @see expireInFlightAggregations for removing in-flight aggregations
    */
  def addInFlightAggregationUpdates(updates: InFlightAggregationUpdates)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Removes all in-flight aggregations that have expired before or at the given timestamp */
  def pruneExpiredInFlightAggregations(upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Retrieve the timestamp of the initial topology snapshot if available. */
  def getInitialTopologySnapshotTimestamp(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]]

  @VisibleForTesting
  protected[state] def numberOfEvents()(implicit
      traceContext: TraceContext
  ): Future[Long]
}

object SequencerStateManagerStore {
  final case class PruningResult(eventsPruned: Long, newMinimumCountersSupported: MemberCounters)
}
