// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.integrations.state

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.data.EphemeralState
import com.digitalasset.canton.domain.sequencing.integrations.state.SequencerStateManagerStore.PruningResult
import com.digitalasset.canton.domain.sequencing.integrations.state.statemanager.MemberSignedEvents
import com.digitalasset.canton.domain.sequencing.sequencer.store.SaveLowerBoundError
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregationUpdates,
  InFlightAggregations,
  InternalSequencerPruningStatus,
  SequencerMemberStatus,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.{Member, UnauthenticatedMemberId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class InMemorySequencerStateManagerStore(
    protected val loggerFactory: NamedLoggerFactory
) extends SequencerStateManagerStore
    with NamedLogging {

  private val state: AtomicReference[State] =
    new AtomicReference[State](State.empty)

  override def readAtBlockTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[EphemeralState] = {
    val snapshot = state.get()

    val head = EphemeralState(
      snapshot.indices.fmap(_.events.filter(_.timestamp <= timestamp).lastOption).collect {
        case (member, Some(lastEvent)) =>
          member -> lastEvent.counter
      },
      snapshot.inFlightAggregations.mapFilter(_.project(timestamp)),
      snapshot.status(Some(timestamp)),
      trafficState =
        snapshot.indices.fmap(_.trafficStates.filter(_.timestamp <= timestamp).lastOption).collect {
          case (member, Some(lastTrafficState)) =>
            member -> lastTrafficState
        },
    )
    Future.successful(head)
  }

  override def readRange(
      member: Member,
      startInclusive: SequencerCounter,
      endExclusive: SequencerCounter,
  )(implicit traceContext: TraceContext): Source[OrdinarySerializedEvent, NotUsed] = {
    ErrorUtil.requireArgument(
      startInclusive < endExclusive,
      "startInclusive must be less than endExclusive",
    )
    val snapshot = state.get()
    val memberIndex =
      snapshot.indices
        .get(member)
        .map(_.events)
        .getOrElse(Seq.empty)
        .filter(_.counter >= startInclusive)
    val results = memberIndex.take((endExclusive - startInclusive).toInt)

    logger.debug(s"Returning ${results.size} results for range [$startInclusive - $endExclusive]")

    Source.fromIterator(() => results.iterator)
  }

  /** Returns all the events in the given time range. */
  private[domain] def allEventsInTimeRange(
      startExclusive: CantonTimestamp,
      endInclusive: CantonTimestamp,
  ): Map[Member, NonEmpty[Seq[OrdinarySerializedEvent]]] = {
    val snapshot = state.get()
    snapshot.indices.mapFilter(index =>
      NonEmpty.from(
        index.events.dropWhile(_.timestamp <= startExclusive).takeWhile(_.timestamp <= endInclusive)
      )
    )
  }

  private[domain] def allRegistrations(): Iterable[(Member, CantonTimestamp)] = {
    val snapshot = state.get()
    snapshot.indices.fmap(_.addedAt)
  }

  private[domain] def inFlightAggregationsAt(timestamp: CantonTimestamp): InFlightAggregations = {
    val snapshot = state.get()
    snapshot.inFlightAggregations.filterNot { case (_, inFlightAggregation) =>
      inFlightAggregation.expired(timestamp)
    }
  }

  private case class MemberIndex(
      addedAt: CantonTimestamp,
      events: Seq[OrdinarySerializedEvent] = Seq.empty,
      lastAcknowledged: Option[CantonTimestamp] = None,
      isEnabled: Boolean = true,
      trafficStates: Seq[TrafficState] = Seq.empty,
  ) {
    def addEvent(event: OrdinarySerializedEvent, trafficState: Option[TrafficState]): MemberIndex =
      copy(events = events :+ event, trafficStates = trafficStates ++ trafficState.toList)

    def disable(): MemberIndex = copy(isEnabled = false)

    def acknowledge(timestamp: CantonTimestamp): MemberIndex = {
      val ts = lastAcknowledged.map(_ max timestamp).getOrElse(timestamp)
      copy(lastAcknowledged = Some(ts))
    }

    def toStatus(member: Member): SequencerMemberStatus =
      SequencerMemberStatus(member, addedAt, lastAcknowledged, isEnabled)
  }

  def locatePruningTimestamp(skip: NonNegativeInt): Option[CantonTimestamp] = {
    val sortedEvents = state.get().indices.values.flatMap(_.events.map(_.timestamp)).toList.sorted
    sortedEvents.lift(skip.value)
  }

  private object State {
    val empty: State =
      State(
        indices = Map.empty,
        pruningLowerBound = None,
        maybeOnboardingTopologyTimestamp = None,
        inFlightAggregations = Map.empty,
      )
  }

  private case class State(
      indices: Map[Member, MemberIndex],
      pruningLowerBound: Option[CantonTimestamp],
      maybeOnboardingTopologyTimestamp: Option[CantonTimestamp],
      inFlightAggregations: InFlightAggregations,
  ) {

    def prune(requestedTimestamp: CantonTimestamp): (State, PruningResult) = {
      val (newIndices, eventsDeleted) =
        indices.toList.foldLeft[(Map[Member, MemberIndex], Long)](
          (Map.empty, 0L)
        ) { case ((newInd, eventsDeleted), (member, index)) =>
          val (deletedEvents, remainingEvents) = index.events.partition(
            _.timestamp < requestedTimestamp
          )
          (
            newInd + (member -> index.copy(events = remainingEvents)),
            eventsDeleted + deletedEvents.size,
          )
        }

      val newMinimumCountersSupported =
        newIndices
          .filter { case (_, memberIndex) => memberIndex.isEnabled }
          .fmap(_.events.headOption)
          .collect {
            case (member, Some(newMinEv))
                if newMinEv.counter > SequencerCounter.Genesis => // filter out cases that did not get affected by pruning
              member -> newMinEv.counter
          }

      (
        this.copy(indices = newIndices, pruningLowerBound = Some(requestedTimestamp)),
        PruningResult(
          eventsPruned = eventsDeleted,
          newMinimumCountersSupported = newMinimumCountersSupported,
        ),
      )
    }

    def status(asOf: Option[CantonTimestamp]): InternalSequencerPruningStatus =
      InternalSequencerPruningStatus(
        lowerBound = pruningLowerBound.getOrElse(CantonTimestamp.Epoch),
        members = indices.toSeq.mapFilter { case (member, index) =>
          Option.when(asOf.forall(index.addedAt <= _))(index.toStatus(member))
        },
      )

    def addMember(member: Member, addedAt: CantonTimestamp): State =
      copy(indices = indices + (member -> MemberIndex(addedAt)))

    def acknowledge(member: Member, timestamp: CantonTimestamp): State =
      copy(indices = indices + (member -> indices(member).acknowledge(timestamp)))

    def lastAcknowledged: Map[Member, CantonTimestamp] =
      indices.collect { case (member, MemberIndex(_, _, Some(lastAcknowledged), true, _)) =>
        member -> lastAcknowledged
      }

    def disableMember(member: Member): State =
      copy(indices = indices + (member -> indices(member).disable()))

    def unregisterUnauthenticatedMember(member: UnauthenticatedMemberId): State =
      copy(indices = indices - member)

    def addEvents(
        events: Map[Member, OrdinarySerializedEvent],
        trafficSate: Map[Member, TrafficState],
    )(implicit traceContext: TraceContext): State = {
      ErrorUtil.requireArgument(
        events.values.map(_.counter).forall(_ >= SequencerCounter.Genesis),
        "all counters must be greater or equal to the genesis counter",
      )

      ErrorUtil.requireArgument(
        events.values.map(_.timestamp).toSet.sizeCompare(1) <= 0,
        "events should all be for the same timestamp",
      )

      events.foldLeft(this) { case (state, (member, event)) =>
        val memberIndex = state.indices.getOrElse(
          member,
          throw new IllegalArgumentException(
            s"member [$member] has not been initialized but has an event"
          ),
        )
        val newTimestamp = event.timestamp
        val newCounter = event.counter
        memberIndex.events.lastOption.foreach { lastEvent =>
          val lastTimestamp = lastEvent.timestamp
          val lastCounter = lastEvent.counter
          ErrorUtil.requireArgument(
            newTimestamp.compareTo(lastTimestamp) > 0,
            s"updated timestamp [$newTimestamp] should be > last timestamp [$lastTimestamp]",
          )
          ErrorUtil.requireArgument(
            lastCounter == newCounter - 1,
            s"previous event had counter [$lastCounter] but new event had counter [$newCounter]",
          )
        }
        state.copy(indices =
          state.indices + (member -> memberIndex.addEvent(event, trafficSate.get(member)))
        )
      }
    }

    def addInFlightAggregationUpdates(
        updates: InFlightAggregationUpdates
    )(implicit traceContext: TraceContext): State =
      this.copy(inFlightAggregations =
        InFlightAggregations.tryApplyUpdates(
          this.inFlightAggregations,
          updates,
          // Persistence must be idempotent and therefore cannot enforce the aggregation errors
          ignoreInFlightAggregationErrors = true,
        )
      )

    def pruneExpiredInFlightAggregations(upToInclusive: CantonTimestamp): State =
      this.copy(inFlightAggregations = this.inFlightAggregations.filterNot {
        case (_id, aggregation) => aggregation.expired(upToInclusive)
      })
  }

  override def addMember(member: Member, addedAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    state.getAndUpdate {
      _.addMember(member, addedAt)
    }
    Future.unit
  }

  override def addEvents(
      events: MemberSignedEvents,
      trafficSate: Map[Member, TrafficState],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    state.getAndUpdate {
      _.addEvents(events, trafficSate)
    }
    Future.unit
  }

  override def acknowledge(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    state.getAndUpdate {
      _.acknowledge(member, timestamp)
    }
    Future.unit
  }

  override protected[state] def latestAcknowledgements()(implicit
      traceContext: TraceContext
  ): Future[Map[Member, CantonTimestamp]] =
    Future.successful(state.get().lastAcknowledged)

  override def disableMember(member: Member)(implicit traceContext: TraceContext): Future[Unit] = {
    state.getAndUpdate {
      _.disableMember(member)
    }
    Future.unit
  }

  def unregisterUnauthenticatedMember(
      member: UnauthenticatedMemberId
  ): Future[Unit] = {
    state.getAndUpdate {
      _.unregisterUnauthenticatedMember(member)
    }
    Future.unit
  }

  override def isEnabled(member: Member)(implicit traceContext: TraceContext): Future[Boolean] =
    Future.successful(state.get().indices.get(member).fold(false)(_.isEnabled))

  override def status(
  )(implicit traceContext: TraceContext): Future[InternalSequencerPruningStatus] =
    Future.successful(statusSync())

  def statusSync(): InternalSequencerPruningStatus =
    state.get().status(None)

  override def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[PruningResult] =
    Future.successful(pruneSync(requestedTimestamp))

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.OptionPartial"))
  def pruneSync(requestedTimestamp: CantonTimestamp): PruningResult = {
    var result: Option[PruningResult] = None
    state.getAndUpdate { st =>
      val (newState, res) = st.prune(requestedTimestamp)
      result = Some(res)
      newState
    }
    result.get
  }

  override protected[state] def numberOfEventsToBeDeletedByPruneAt(
      requestedTimestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): Future[Long] = Future.successful(
    state.get().indices.map(_._2.events.count(_.timestamp < requestedTimestamp)).sum.toLong
  )
  override def fetchLowerBound()(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = Future.successful(state.get().pruningLowerBound)

  override def saveLowerBound(
      ts: CantonTimestamp,
      maybeOnboardingTopologyTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SaveLowerBoundError, Unit] =
    EitherT(Future.successful(update { state =>
      val newTs = state.pruningLowerBound.map(_ max ts).getOrElse(ts)
      Either.cond(
        newTs == ts,
        state.copy(
          pruningLowerBound = Some(newTs),
          // Only set onboarding topology timestamp if we have not set a lower bound before
          maybeOnboardingTopologyTimestamp = state.pruningLowerBound.fold(
            maybeOnboardingTopologyTimestamp
          )(_ => state.maybeOnboardingTopologyTimestamp),
        ),
        SaveLowerBoundError.BoundLowerThanExisting(newTs, ts),
      )
    }))

  override def addInFlightAggregationUpdates(updates: InFlightAggregationUpdates)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.successful {
    state.getAndUpdate(_.addInFlightAggregationUpdates(updates)).discard[State]
  }

  override def pruneExpiredInFlightAggregations(upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.successful {
    pruneExpiredInFlightAggregationsInternal(upToInclusive).discard[InFlightAggregations]
  }

  private[domain] def pruneExpiredInFlightAggregationsInternal(
      upToInclusive: CantonTimestamp
  ): InFlightAggregations =
    state.updateAndGet(_.pruneExpiredInFlightAggregations(upToInclusive)).inFlightAggregations

  private def update[E](update: State => Either[E, State]): Either[E, Unit] = {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var error: Option[E] = None
    state.getAndUpdate { state =>
      update(state)
        .fold(
          err => {
            error = Some(err)
            state
          },
          identity,
        )
    }
    error.toLeft(())
  }

  override def getInitialTopologySnapshotTimestamp(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    Future.successful(state.get().maybeOnboardingTopologyTimestamp)
}
