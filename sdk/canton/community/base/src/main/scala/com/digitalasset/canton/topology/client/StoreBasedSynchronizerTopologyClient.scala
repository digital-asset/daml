// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.time.{Clock, TimeAwaiter}
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{
  PackageDependencyResolver,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.time.Duration as JDuration
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success
import scala.util.control.NonFatal

trait TopologyAwaiter extends FlagCloseable {

  this: SynchronizerTopologyClientWithInit =>

  protected def clock: Clock
  private val conditions = new AtomicReference[Seq[StateAwait]](Seq.empty)

  override protected def onClosed(): Unit = {
    super.onClosed()
    shutdownConditions()
  }

  private def shutdownConditions(): Unit =
    conditions.updateAndGet { x =>
      x.foreach(_.promise.trySuccess(UnlessShutdown.AbortedDueToShutdown).discard[Boolean])
      Seq()
    }.discard

  protected def checkAwaitingConditions()(implicit traceContext: TraceContext): Unit =
    conditions
      .get()
      .foreach(stateAwait =>
        try { stateAwait.check() }
        catch {
          case NonFatal(e) =>
            logger.error("An exception occurred while checking awaiting conditions.", e)
            stateAwait.promise.tryFailure(e).discard[Boolean]
        }
      )

  private class StateAwait(func: => FutureUnlessShutdown[Boolean]) {
    val promise: Promise[UnlessShutdown[Boolean]] = Promise[UnlessShutdown[Boolean]]()
    promise.future.onComplete { _ =>
      val _ = conditions.updateAndGet(_.filterNot(_.promise.isCompleted))
    }

    def check(): Unit =
      if (!promise.isCompleted) {
        // Ok to use onComplete as any exception will be propagated to the promise.
        func.unwrap.onComplete {
          case Success(UnlessShutdown.Outcome(false)) => // nothing to do, will retry later
          case res =>
            val _ = promise.tryComplete(res)
        }
      }
  }

  private[topology] def scheduleAwait(
      condition: => FutureUnlessShutdown[Boolean],
      timeout: Duration,
  ): FutureUnlessShutdown[Boolean] = {
    val waiter = new StateAwait(condition)
    conditions.updateAndGet(_ :+ waiter)
    if (!isClosing) {
      if (timeout.isFinite) {
        clock
          .scheduleAfter(
            _ => waiter.promise.trySuccess(UnlessShutdown.Outcome(false)).discard,
            JDuration.ofMillis(timeout.toMillis),
          )
          .discard
      }
      waiter.check()
    } else {
      // calling shutdownConditions() will ensure all added conditions are marked as aborted due to shutdown
      // ensure we don't have a race condition between isClosing and updating conditions
      shutdownConditions()
    }
    FutureUnlessShutdown(waiter.promise.future)
  }
}

/** The synchronizer topology client that reads data from a topology store
  *
  * @param synchronizerId
  *   The synchronizer id corresponding to this store
  * @param store
  *   The store
  */
class StoreBasedSynchronizerTopologyClient(
    val clock: Clock,
    val staticSynchronizerParameters: StaticSynchronizerParameters,
    store: TopologyStore[TopologyStoreId.SynchronizerStore],
    packageDependencyResolver: PackageDependencyResolver,
    topologyConfig: TopologyConfig,
    override val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends SynchronizerTopologyClientWithInit
    with TopologyAwaiter
    with NamedLogging {

  def psid: PhysicalSynchronizerId = store.storeId.psid
  val synchronizerId: SynchronizerId = psid.logical

  private val effectiveTimeAwaiter =
    new TimeAwaiter(
      getCurrentKnownTime = () => topologyKnownUntilTimestamp,
      timeouts,
      loggerFactory,
    )

  private val sequencedTimeAwaiter =
    new TimeAwaiter(
      // waiting for a sequenced time has "inclusive" semantics
      getCurrentKnownTime = () => head.get().sequencedTimestamp.value,
      timeouts,
      loggerFactory,
    )

  private val pendingChanges = new AtomicInteger(0)

  private case class HeadTimestamps(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
  ) {
    def update(
        newSequencedTimestamp: SequencedTime,
        newEffectiveTimestamp: EffectiveTime,
        newApproximateTimestamp: ApproximateTime,
    ): HeadTimestamps =
      HeadTimestamps(
        sequencedTimestamp =
          SequencedTime(sequencedTimestamp.value.max(newSequencedTimestamp.value)),
        effectiveTimestamp =
          EffectiveTime(effectiveTimestamp.value.max(newEffectiveTimestamp.value)),
        approximateTimestamp =
          ApproximateTime(approximateTimestamp.value.max(newApproximateTimestamp.value)),
      )
  }
  private val head = new AtomicReference[HeadTimestamps](
    HeadTimestamps(
      SequencedTime(CantonTimestamp.MinValue),
      EffectiveTime(CantonTimestamp.MinValue),
      ApproximateTime(CantonTimestamp.MinValue),
    )
  )

  override def updateHead(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(
      s"Head update: sequenced=$sequencedTimestamp, effective=$effectiveTimestamp, approx=$approximateTimestamp"
    )
    val curHead =
      head.updateAndGet(_.update(sequencedTimestamp, effectiveTimestamp, approximateTimestamp))
    // waiting for a sequenced time has "inclusive" semantics
    sequencedTimeAwaiter.notifyAwaitedFutures(curHead.sequencedTimestamp.value)
    // now notify the futures that wait for this update here. as the update is active at t+epsilon, (see most recent timestamp),
    // we'll need to notify accordingly
    effectiveTimeAwaiter.notifyAwaitedFutures(curHead.effectiveTimestamp.value.immediateSuccessor)
  }

  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(
      s"Observed: sequenced=$sequencedTimestamp, effective=$effectiveTimestamp, numTransactions=${transactions.size}"
    )
    observedInternal(sequencedTimestamp, effectiveTimestamp)
    FutureUnlessShutdown.unit
  }

  override def numPendingChanges: Int = pendingChanges.get()

  private def observedInternal(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
  )(implicit traceContext: TraceContext): Unit = {

    def updateHeadWithApproximateTime(approximateTime: CantonTimestamp): Unit =
      updateHead(
        sequencedTimestamp,
        effectiveTimestamp,
        ApproximateTime(approximateTime),
      )

    // Advance the head timestamps; the head approximate timestamp can be advanced to the observed sequenced timestamp.
    updateHeadWithApproximateTime(sequencedTimestamp.value)

    checkAwaitingConditions()

    // If needed, use the synchronizer time tracker to asynchronously observe the effective timestamp being reached
    //  on the synchronizer, and advance the head approximate timestamp to the head effective timestamp.
    if (effectiveTimestamp.value > sequencedTimestamp.value) {
      pendingChanges.incrementAndGet()

      synchronizerTimeTracker.get.fold {
        logger.warn(
          "Not advancing approximate time to effective time using the time tracker as it's unavailable"
        )
      } { timeTracker =>
        // TODO(#28010): replace time proofs with a more scalable mechanism
        // This currently sequences time proofs to observe reaching the effective timestamp,
        //  so it does not scale well, as many members may trigger time proofs around the same time
        //  and may thus flood sequencers.
        //  However, as a mitigation, the time tracker sends a time proof only if it's not cancelled,
        //  the observation time has elapsed, and the time-advancing broadcast was ineffective
        //  (e.g., was triggered too early).

        // When the effective timestamp is reached on the synchronizer,
        // only the head approximate timestamp is actually advanced,
        // because the head sequenced and effective timestamps
        // have already been advanced synchronously above.

        def advanceApproximateTimeToEffectiveTime(): Unit = {
          updateHeadWithApproximateTime(effectiveTimestamp.value)
          if (pendingChanges.decrementAndGet() == 0) {
            logger.debug(
              s"Effective at $effectiveTimestamp, there are no more pending topology changes (last were from $sequencedTimestamp)"
            )
          }
          checkAwaitingConditions()
        }

        if (topologyConfig.useTimeProofsToObserveEffectiveTime)
          timeTracker
            .awaitTick(effectiveTimestamp.value)
            .fold {
              // Effective time already reached on the synchronizer
              advanceApproximateTimeToEffectiveTime()
            } { awaitTickF =>
              awaitTickF.foreach(_ => advanceApproximateTimeToEffectiveTime())
            }
      }
    }
  }

  /** Returns whether a snapshot for the given timestamp is available. */
  override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
    topologyKnownUntilTimestamp >= timestamp

  override protected[topology] def trySnapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): StoreBasedTopologySnapshot = {
    ErrorUtil.requireArgument(
      timestamp <= topologyKnownUntilTimestamp,
      s"requested snapshot=$timestamp, topology known until=$topologyKnownUntilTimestamp",
    )
    new StoreBasedTopologySnapshot(timestamp, store, packageDependencyResolver, loggerFactory)
  }

  def findTopologyIntervalForTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(EffectiveTime, Option[EffectiveTime])]] =
    store.findTopologyIntervalForTimestamp(timestamp)

  override def snapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[TopologySnapshotLoader] =
    waitForTimestampWithLogging(timestamp).map(_ => trySnapshot(timestamp))

  override def hypotheticalSnapshot(
      timestamp: CantonTimestamp,
      desiredTimestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[StoreBasedTopologySnapshot] =
    ErrorUtil.internalError(
      new UnsupportedOperationException(
        "tryHypotheticalSnapshot is not " +
          "supported on store-based synchronizer topology clients"
      )
    )

  /** @return
    *   the timestamp as of which the latest known effective time will be valid, i.e.
    *   latestKnownEffectiveTimestamp.immediateSuccessor
    */
  override def topologyKnownUntilTimestamp: CantonTimestamp =
    head.get().effectiveTimestamp.value.immediateSuccessor

  /** returns the current approximate timestamp
    *
    * whenever we get an update, we do set the approximate timestamp first to the sequencer time and
    * use the synchronizer time tracker to advance the approximate time to the effective time after
    * a timestamp greater than the effective time was received from the sequencer.
    */
  override def approximateTimestamp: CantonTimestamp =
    head.get().approximateTimestamp.value.immediateSuccessor

  override def awaitTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Option[FutureUnlessShutdown[Unit]] =
    effectiveTimeAwaiter.awaitKnownTimestamp(timestamp)

  override def awaitSequencedTimestamp(timestampInclusive: SequencedTime)(implicit
      traceContext: TraceContext
  ): Option[FutureUnlessShutdown[Unit]] =
    sequencedTimeAwaiter.awaitKnownTimestamp(timestampInclusive.value)

  override def awaitMaxTimestamp(sequencedTime: SequencedTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    for {
      // We wait for the sequenced time to be processed to ensure that `maxTimestamp`'s output is stable.
      _ <- awaitSequencedTimestamp(sequencedTime).getOrElse(
        FutureUnlessShutdown.unit
      )
      maxTimestamp <-
        store.maxTimestamp(sequencedTime, includeRejected = false)
    } yield maxTimestamp

  override protected def onClosed(): Unit = {
    LifeCycle.close(
      sequencedTimeAwaiter,
      effectiveTimeAwaiter,
    )(logger)
    super.onClosed()
  }

  override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    scheduleAwait(
      FutureUnlessShutdown.outcomeF(condition(currentSnapshotApproximation)),
      timeout,
    )

  override def awaitUS(
      condition: TopologySnapshot => FutureUnlessShutdown[Boolean],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    scheduleAwait(condition(currentSnapshotApproximation), timeout)

}
