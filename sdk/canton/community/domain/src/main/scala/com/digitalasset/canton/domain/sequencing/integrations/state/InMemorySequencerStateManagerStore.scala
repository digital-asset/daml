// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.integrations.state

import cats.syntax.functorFilter.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregationUpdates,
  InFlightAggregations,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class InMemorySequencerStateManagerStore(
    protected val loggerFactory: NamedLoggerFactory
) extends SequencerStateManagerStore
    with NamedLogging {

  private val state: AtomicReference[State] =
    new AtomicReference[State](State.empty)

  override def readInFlightAggregations(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[InFlightAggregations] = {
    val snapshot = state.get()
    val inFlightAggregations = snapshot.inFlightAggregations.mapFilter(_.project(timestamp))
    Future.successful(inFlightAggregations)
  }

  private case class MemberIndex(
      addedAt: CantonTimestamp,
      events: Seq[OrdinarySerializedEvent] = Seq.empty,
      lastAcknowledged: Option[CantonTimestamp] = None,
      isEnabled: Boolean = true,
  )

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
        case (_, aggregation) => aggregation.expired(upToInclusive)
      })
  }

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
}
