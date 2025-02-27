// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.*
import com.digitalasset.canton.topology.store.TopologyStore.Change
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

/** Computes the effective timestamps of topology transactions
  *
  * Transaction validation and processing depends on the topology state at the given sequencing
  * time. If topology transactions became effective immediately, we would have to inspect every
  * event first if there is a topology state and wait until all the topology processing has finished
  * before evaluating the transaction. This would cause a sequential bottleneck.
  *
  * To avoid this bottleneck, topology transactions become effective only in the future at an
  * "effective time", where effective time >= sequencingTime +
  * synchronizerParameters.topologyChangeDelay.
  *
  * However, the synchronizerParameters can change and so can the topologyChangeDelay. So it is
  * non-trivial to apply the right topologyChangeDelay. Also, if the topologyChangeDelay is
  * decreased, the effective timestamps of topology transactions could "run backwards", which would
  * break topology management.
  *
  * This class computes the effective timestamps of topology transactions from their sequencing
  * timestamps. It makes sure that effective timestamps are strictly monotonically increasing. For
  * better performance, it keeps the current as well as all future topologyChangeDelays in memory,
  * so that all computations can be performed without reading from the database.
  */
class TopologyTimestampPlusEpsilonTracker(
    store: TopologyStore[TopologyStoreId.SynchronizerStore],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  /** Stores a topologyChangeDelay together with its validity. */
  private case class State(topologyChangeDelay: NonNegativeFiniteDuration, validFrom: EffectiveTime)

  /** The currently active as well as all future states, sorted by `State.validFrom` in descending
    * order
    */
  private val states = new AtomicReference[List[State]](List.empty)

  /** The maximum effective time assigned to a topology transaction. Used to enforce that the
    * effective time is strictly monotonically increasing.
    *
    * A value of `EffectiveTime.MaxValue` indicates that the tracker has not yet been initialized.
    */
  private val maximumEffectiveTime =
    new AtomicReference[EffectiveTime](EffectiveTime.MaxValue)

  /** Computes the effective time for a given sequencing time. The computed effective time is
    * strictly monotonically increasing if requested and monotonically increasing otherwise.
    *
    * The computed effective time is at least sequencingTime + topologyChangeDelay(sequencingTime).
    *
    * The methods of this tracker must be called as follows:
    *   1. Make sure the topologyStore contains all topology transactions effective at
    *      `sequencedTime`.
    *   1. Call trackAndComputeEffectiveTime for the first sequenced event. This will also
    *      initialize the tracker.
    *   1. Call adjustTopologyChangeDelay, if the sequenced event at `sequencedTime` contains a
    *      valid topology transaction that changes the topologyChangeDelay.
    *   1. Call trackAndComputeEffectiveTime for the next sequenced event.
    *   1. Go to 3.
    *
    * @param strictMonotonicity
    *   whether the returned effective time must be strictly greater than the previous one computed.
    *   As it changes internal state, all synchronizer members must call
    *   `trackAndComputeEffectiveTime(sequencedTime, true)` for exactly the same `sequencedTime`s
    *   (in ascending order). In practice, `strictMonotonicity` should be true iff the underlying
    *   sequenced event contains at least one topology transaction, it is addressed to
    *   [[com.digitalasset.canton.sequencing.protocol.AllMembersOfSynchronizer]] and it has no
    *   topologyTimestamp.
    */
  def trackAndComputeEffectiveTime(sequencedTime: SequencedTime, strictMonotonicity: Boolean)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[EffectiveTime] = for {
    // initialize the tracker, if necessary
    _ <-
      if (maximumEffectiveTime.get() == EffectiveTime.MaxValue)
        initialize(sequencedTime)
      else FutureUnlessShutdown.unit
  } yield {
    cleanup(sequencedTime)

    // This is sequencedTime + topologyChangeDelay(sequencedTime)
    val rawEffectiveTime = rawEffectiveTimeOf(sequencedTime)

    if (strictMonotonicity) {
      // All synchronizer members run this branch for the same sequenced times.
      // Therefore, all members will update maximumEffectiveTime in the same way.

      val effectiveTime =
        maximumEffectiveTime.updateAndGet(_.immediateSuccessor() max rawEffectiveTime)

      if (effectiveTime != rawEffectiveTime) {
        // For simplicity, let's allow the synchronizer to decrease the topologyChangeDelay arbitrarily.
        // During a transition period, the effective time needs to be corrected.
        logger.info(
          s"Computed effective time of $rawEffectiveTime would go backwards. Using $effectiveTime instead."
        )
      }
      effectiveTime
    } else {
      // We do not update the state here, as different members run this piece of codes with different sequencing times.
      // The effective times computed here are monotonically increasing, even when rawEffectiveTime goes backwards,
      // as we bump maximumEffectiveTime whenever a SynchronizerParameterState transaction expires.
      maximumEffectiveTime.get() max rawEffectiveTime
    }
  }

  private def initialize(
      sequencedTime: SequencedTime
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = for {
    // find the current and upcoming change delays
    currentAndUpcomingChangeDelays <- store.findCurrentAndUpcomingChangeDelays(sequencedTime.value)
    currentChangeDelay = currentAndUpcomingChangeDelays.last1

    // Find the maximum stored effective time for initialization of maximumEffectiveTime.
    // Note that this must include rejections and proposals,
    // as trackAndComputeEffectiveTime may change maximumEffectiveTime based on proposals / rejections.
    maximumTimestampInStoreO <- store.maxTimestamp(sequencedTime.value, includeRejected = true)
    maximumEffectiveTimeInStore = maximumTimestampInStoreO
      .map { case (_, effective) => effective }
      .getOrElse(EffectiveTime.MinValue)

    // Find all topologyChangeDelay transactions that have expired
    // - at or after sequencing the current delay
    // - before sequencedTime (so that the expiry is in the past)
    expiredTopologyDelays <- store.findExpiredChangeDelays(
      validUntilMinInclusive = currentChangeDelay.sequenced.value,
      validUntilMaxExclusive = sequencedTime.value,
    )
  } yield {

    // Initialize state based on current and upcoming change delays
    val initialStates = currentAndUpcomingChangeDelays.map {
      case Change.TopologyDelay(_, validFrom, _, changeDelay) => State(changeDelay, validFrom)
    }
    logger.info(s"Initializing with $initialStates...")
    states.set(initialStates)

    // Initialize maximumEffectiveTime based on the maximum effective time in the store.
    // This will cover all adjustments made within trackAndComputeEffectiveTime.
    logger.info(s"Maximum effective time in store is $maximumEffectiveTimeInStore.")
    maximumEffectiveTime.set(maximumEffectiveTimeInStore)

    // Make sure that maximumEffectiveTime is at least:
    // - the maximum effective time a change delay can "produce"
    // - taken over all expired topology change delays.
    // This will cover all adjustments made within cleanup.
    val maximumEffectiveTimeAtExpiryO = expiredTopologyDelays.collect {
      case Change.TopologyDelay(_, _, Some(validUntil), changeDelay) =>
        validUntil + changeDelay
    }.maxOption
    logger.info(s"Maximum effective time at expiry is $maximumEffectiveTimeAtExpiryO.")
    maximumEffectiveTimeAtExpiryO.foreach(t => maximumEffectiveTime.updateAndGet(_ max t).discard)
  }

  /** Adds the correct `topologyChangeDelay` to the given `sequencingTime`. The resulting effective
    * time may be non-monotonic.
    */
  private def rawEffectiveTimeOf(
      sequencingTime: SequencedTime
  )(implicit traceContext: TraceContext): EffectiveTime = {
    val topologyChangeDelay = states
      .get()
      .collectFirst {
        case state if sequencingTime.value > state.validFrom.value =>
          state.topologyChangeDelay
      }
      .getOrElse(
        ErrorUtil.internalError(
          new IllegalArgumentException(
            s"Unable to determine effective time for $sequencingTime. State: ${states.get()}"
          )
        )
      )

    EffectiveTime(sequencingTime.value) + topologyChangeDelay
  }

  /** Remove states that have expired before sequencedTime. Bump maximumEffectiveTime to
    * topologyChangeDelay + validUntil(topologyChangeDelay), if topologyChangeDelay has expired.
    */
  private def cleanup(sequencedTime: SequencedTime): Unit = {
    val oldStates = states
      .getAndUpdate { oldStates =>
        val (futureStates, pastStates) = oldStates.span(_.validFrom.value >= sequencedTime.value)

        val currentStateO = pastStates.headOption
        futureStates ++ currentStateO.toList
      }

    val pastStates: List[State] = oldStates.filter(_.validFrom.value < sequencedTime.value)
    pastStates
      .sliding(2)
      .toSeq
      .collect { case Seq(next, prev) =>
        // Note that next.validFrom == prev.validUntil.
        maximumEffectiveTime.getAndUpdate(_ max next.validFrom + prev.topologyChangeDelay).discard
      }
      .discard
  }

  /** Inform the tracker about a potential change to topologyChangeDelay. Must be called whenever a
    * [[com.digitalasset.canton.topology.transaction.SynchronizerParametersState]] is committed.
    * Must not be called for rejections, proposals, and transactions that expire immediately.
    *
    * @throws java.lang.IllegalArgumentException
    *   if effectiveTime is not strictly monotonically increasing
    */
  def adjustTopologyChangeDelay(
      effectiveTime: EffectiveTime,
      newTopologyChangeDelay: NonNegativeFiniteDuration,
  )(implicit traceContext: TraceContext): Unit =
    states
      .getAndUpdate { oldStates =>
        val oldHeadState = oldStates.headOption
        val newHeadState = State(newTopologyChangeDelay, effectiveTime)
        ErrorUtil.requireArgument(
          oldHeadState.forall(_.validFrom.value < effectiveTime.value),
          s"Invalid adjustment of topologyChangeDelay from $oldHeadState to $newHeadState",
        )
        logger.info(s"Updated topology change delay from=$oldHeadState to $newHeadState.")
        newHeadState +: oldStates
      }
      .discard[List[State]]
}
