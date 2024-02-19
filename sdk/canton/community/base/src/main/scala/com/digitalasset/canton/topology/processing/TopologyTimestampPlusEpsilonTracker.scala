// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.time.*
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.DomainParametersStateX
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** Compute and synchronise the effective timestamps
  *
  * Transaction validation and processing depends on the topology state at the given sequencer time.
  * Therefore, we would have to inspect every event first if there is a topology state and wait until all
  * the topology processing has finished before evaluating the transaction. This would be slow and sequential.
  *
  * Therefore, we future date our topology transactions with an "effective time", computed from
  * the sequencerTime + domainParameters.topologyChangeDelay.
  *
  * However, the domainParameters can change and so can the topologyChangeDelay. Therefore we need to be a bit careful
  * when computing the effective time and track the changes to the topologyChangeDelay parameter accordingly.
  *
  * This class (hopefully) takes care of this logic
  */
class TopologyTimestampPlusEpsilonTracker(
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
) extends NamedLogging
    with TimeAwaiter
    with FlagCloseable {

  override protected def onClosed(): Unit = {
    expireTimeAwaiter()
    super.onClosed()
  }

  /** track changes to epsilon carefully.
    *
    * increasing the epsilon is straight forward. reducing it requires more care
    *
    * @param epsilon the epsilon is the time we add to the timestamp
    * @param validFrom from when on should this one be valid (as with all topology transactions, the time is exclusive)
    */
  private case class State(epsilon: NonNegativeFiniteDuration, validFrom: EffectiveTime)

  /** sorted list of epsilon updates (in descending order of sequencing) */
  private val state = new AtomicReference[List[State]](List())

  /** protect us against broken domains that send topology transactions in times when they've just reduced the
    * epsilon in a way that could lead the second topology transaction to take over the epsilon change.
    */
  private val uniqueUpdateTime = new AtomicReference[EffectiveTime](EffectiveTime.MinValue)

  private val lastEffectiveTimeProcessed =
    new AtomicReference[EffectiveTime](EffectiveTime.MinValue)

  private val sequentialWait =
    new AtomicReference[FutureUnlessShutdown[EffectiveTime]](
      FutureUnlessShutdown.pure(EffectiveTime.MinValue)
    )

  override protected def currentKnownTime: CantonTimestamp = lastEffectiveTimeProcessed.get().value

  private def adjustByEpsilon(
      sequencingTime: SequencedTime
  )(implicit traceContext: TraceContext): EffectiveTime = {
    @tailrec
    def go(items: List[State]): NonNegativeFiniteDuration = items match {
      case item :: _ if sequencingTime.value > item.validFrom.value =>
        item.epsilon
      case last :: Nil =>
        if (sequencingTime.value < last.validFrom.value)
          logger.error(
            s"Bad sequencing time $sequencingTime with last known epsilon update at ${last}"
          )
        last.epsilon
      case Nil =>
        logger.error(
          s"Epsilon tracker is not initialised at sequencing time ${sequencingTime}, will use default value ${DynamicDomainParameters.topologyChangeDelayIfAbsent}"
        )
        DynamicDomainParameters.topologyChangeDelayIfAbsent // we use this (0) as a safe default
      case _ :: rest => go(rest)
    }
    val epsilon = go(state.get())
    EffectiveTime(sequencingTime.value.plus(epsilon.duration))
  }

  // must call effectiveTimeProcessed in due time
  def adjustTimestampForUpdate(sequencingTime: SequencedTime)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[EffectiveTime] =
    synchronize(
      sequencingTime, {
        val adjusted = adjustByEpsilon(sequencingTime)
        val monotonic = {
          // if a broken domain manager sends us an update too early after an epsilon reduction, we'll catch that and
          // ensure that we don't store txs in an out of order way
          // i.e. if we get at t1 an update with epsilon_1 < epsilon_0, then we do have to ensure that no topology
          // transaction is sent until t1 + (epsilon_0 - epsilon_1) (as this is the threshold for any message just before t1)
          // but if the topology manager still does it, we'll just work-around
          uniqueUpdateTime.updateAndGet(cur =>
            if (cur.value >= adjusted.value) EffectiveTime(cur.value.immediateSuccessor)
            else adjusted
          )
        }
        if (monotonic != adjusted) {
          logger.error(
            s"Broken or malicious domain topology manager is sending transactions during epsilon changes at ts=$sequencingTime!"
          )
        }
        monotonic
      },
    )

  private def synchronize(
      sequencingTime: SequencedTime,
      computeEffective: => EffectiveTime,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[EffectiveTime] = {
    // note, this is a side effect free chain await
    def chainUpdates(previousEffectiveTime: EffectiveTime): FutureUnlessShutdown[EffectiveTime] = {
      FutureUnlessShutdown(
        FutureUtil.logOnFailure(
          {
            val synchronizeAt = previousEffectiveTime.value.min(sequencingTime.value)
            awaitKnownTimestampUS(synchronizeAt) match {
              case None => FutureUnlessShutdown.pure(computeEffective)
              case Some(value) =>
                logger.debug(
                  s"Need to wait until topology processing has caught up at $sequencingTime (must reach $synchronizeAt with current=${currentKnownTime})"
                )
                value.map { _ =>
                  logger.debug(s"Topology processing caught up at $sequencingTime")
                  computeEffective
                }
            }
          }.unwrap,
          "chaining of sequential waits failed",
        )
      )
    }
    val nextChainP = new PromiseUnlessShutdown[EffectiveTime](
      "synchronized-chain-promise",
      futureSupervisor,
    )
    val ret =
      sequentialWait.getAndUpdate(cur => cur.flatMap(_ => FutureUnlessShutdown(nextChainP.future)))
    ret.onComplete {
      case Success(UnlessShutdown.AbortedDueToShutdown) =>
        nextChainP.shutdown()
      case Success(UnlessShutdown.Outcome(previousEffectiveTime)) =>
        chainUpdates(previousEffectiveTime)
          .map { effectiveTime =>
            nextChainP.outcome(effectiveTime)
          }
          .onShutdown(nextChainP.shutdown())
          .discard
      case Failure(exception) => nextChainP.failure(exception)
    }
    nextChainP.futureUS
  }

  def effectiveTimeProcessed(effectiveTime: EffectiveTime): Unit = {
    val updated = lastEffectiveTimeProcessed.updateAndGet(_.max(effectiveTime))
    notifyAwaitedFutures(updated.value)
  }

  def adjustTimestampForTick(sequencingTime: SequencedTime)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[EffectiveTime] = synchronize(
    sequencingTime, {
      val adjusted = adjustByEpsilon(sequencingTime)
      val monotonic = uniqueUpdateTime.updateAndGet(_.max(adjusted))
      effectiveTimeProcessed(monotonic)
      monotonic
    },
  )

  /** adjust epsilon if it changed
    *
    * @return [[scala.None$]] if epsilon is unchanged or wasn't set before (e.g. when called for the first time),
    *         otherwise [[scala.Some$]] the previous epsilon
    */
  def adjustEpsilon(
      effectiveTime: EffectiveTime,
      sequencingTime: SequencedTime,
      epsilon: NonNegativeFiniteDuration,
  )(implicit traceContext: TraceContext): Option[NonNegativeFiniteDuration] = {
    val oldStates = state.get()
    val currentState = oldStates.headOption
    val ext = State(epsilon, effectiveTime)
    ErrorUtil.requireArgument(
      currentState.forall(_.validFrom.value < effectiveTime.value),
      s"Invalid epsilon adjustment from $currentState to $ext",
    )
    if (!currentState.exists(_.epsilon == epsilon)) {
      // we prepend this new datapoint and
      // keep everything which is not yet valid and the first item which is valid before the sequencing time
      val (effectivesAtOrAfterSequencing, effectivesBeforeSequencing) =
        oldStates.span(_.validFrom.value >= sequencingTime.value)
      val newStates =
        ext +: (effectivesAtOrAfterSequencing ++ effectivesBeforeSequencing.headOption.toList)

      if (!state.compareAndSet(oldStates, newStates)) {
        ErrorUtil.internalError(
          new ConcurrentModificationException(
            s"Topology change delay was updated concurrently. Effective time $effectiveTime, sequencing time $sequencingTime, epsilon $epsilon"
          )
        )
      }
      currentState.map(_.epsilon)
    } else None
  }

}

object TopologyTimestampPlusEpsilonTracker {

  /** Initialize tracker
    *
    * @param processorTs Timestamp strictly (just) before the first message that will be passed:
    *                    No sequenced events may have been passed in earlier crash epochs whose
    *                    timestamp is strictly between `processorTs` and the first message that
    *                    will be passed if these events affect the topology change delay.
    *                    Normally, it's the timestamp of the last message that was successfully
    *                    processed before the one that will be passed first.
    */
  def initializeX(
      tracker: TopologyTimestampPlusEpsilonTracker,
      store: TopologyStoreX[TopologyStoreId.DomainStore],
      processorTs: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[EffectiveTime] = for {
    // find the epsilon of a dpc asOf processorTs (which means it is exclusive)
    epsilonAtProcessorTs <- epsilonForTimestamp(store, processorTs)
    // find also all upcoming changes which have effective >= processorTs && sequenced <= processorTs
    // the change that makes up the epsilon at processorTs would be grabbed by the statement above
    upcoming <- tracker.performUnlessClosingF(functionFullName)(
      store
        .findUpcomingEffectiveChanges(processorTs)
        .map(_.collect {
          case tdc: TopologyStoreX.Change.TopologyDelay
              // filter anything out that might be replayed
              if tdc.sequenced.value <= processorTs =>
            tdc
        })
    )
    allPending = (epsilonAtProcessorTs +: upcoming).sortBy(_.sequenced)
    _ = {
      tracker.logger.debug(
        s"Initialising with $allPending"
      )
      // Now, replay all the older epsilon updates that might get activated shortly
      allPending.foreach { change =>
        tracker
          .adjustEpsilon(
            change.effective,
            change.sequenced,
            change.epsilon,
          )
          .discard[Option[NonNegativeFiniteDuration]]
      }
    }
    eff <- tracker.adjustTimestampForTick(SequencedTime(processorTs))
  } yield eff

  def epsilonForTimestamp(
      store: TopologyStoreX[TopologyStoreId.DomainStore],
      asOfExclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[TopologyStoreX.Change.TopologyDelay] = {
    FutureUnlessShutdown
      .outcomeF(
        store
          .findPositiveTransactions(
            asOf = asOfExclusive,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(DomainParametersStateX.code),
            filterUid = None,
            filterNamespace = None,
          )
      )
      .map { txs =>
        txs.result
          .map(x => (x.mapping, x))
          .collectFirst { case (change: DomainParametersStateX, tx) =>
            TopologyStoreX.Change.TopologyDelay(
              tx.sequenced,
              tx.validFrom,
              change.parameters.topologyChangeDelay,
            )
          }
          .getOrElse(
            TopologyStoreX.Change.TopologyDelay(
              SequencedTime(CantonTimestamp.MinValue),
              EffectiveTime(CantonTimestamp.MinValue),
              DynamicDomainParameters.topologyChangeDelayIfAbsent,
            )
          )
      }
  }

}
