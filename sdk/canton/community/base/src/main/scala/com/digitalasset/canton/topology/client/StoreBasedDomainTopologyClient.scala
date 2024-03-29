// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.lf.data.Ref.PackageId
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.time.{Clock, TimeAwaiter}
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, SequencerCounter}

import java.time.Duration as JDuration
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}
import scala.util.Success
import scala.util.control.NonFatal

trait TopologyAwaiter extends FlagCloseable {

  this: DomainTopologyClientWithInit =>

  protected def clock: Clock
  private val conditions = new AtomicReference[Seq[StateAwait]](Seq.empty)

  override protected def onClosed(): Unit = {
    super.onClosed()
    shutdownConditions()
  }

  private def shutdownConditions(): Unit = {
    conditions.updateAndGet { x =>
      x.foreach(_.promise.trySuccess(UnlessShutdown.AbortedDueToShutdown).discard[Boolean])
      Seq()
    }.discard
  }

  protected def checkAwaitingConditions()(implicit traceContext: TraceContext): Unit = {
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
  }

  private class StateAwait(func: => Future[Boolean]) {
    val promise: Promise[UnlessShutdown[Boolean]] = Promise[UnlessShutdown[Boolean]]()
    promise.future.onComplete(_ => {
      val _ = conditions.updateAndGet(_.filterNot(_.promise.isCompleted))
    })

    def check(): Unit = {
      if (!promise.isCompleted) {
        // Ok to use onComplete as any exception will be propagated to the promise.
        func.onComplete {
          case Success(false) => // nothing to do, will retry later
          case res =>
            val _ = promise.tryComplete(res.map(UnlessShutdown.Outcome(_)))
        }
      }
    }
  }

  private[topology] def scheduleAwait(
      condition: => Future[Boolean],
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

// TODO(#15161) collapse with base trait
abstract class BaseDomainTopologyClientX
    extends BaseDomainTopologyClient
    with DomainTopologyClientWithInitX {
  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    observedInternal(sequencedTimestamp, effectiveTimestamp)
}

abstract class BaseDomainTopologyClient
    extends DomainTopologyClientWithInit
    with TopologyAwaiter
    with TimeAwaiter {

  def protocolVersion: ProtocolVersion

  private val pendingChanges = new AtomicInteger(0)

  private case class HeadTimestamps(
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
  ) {
    def update(
        newEffectiveTimestamp: EffectiveTime,
        newApproximateTimestamp: ApproximateTime,
    ): HeadTimestamps = {
      HeadTimestamps(
        effectiveTimestamp =
          EffectiveTime(effectiveTimestamp.value.max(newEffectiveTimestamp.value)),
        approximateTimestamp =
          ApproximateTime(approximateTimestamp.value.max(newApproximateTimestamp.value)),
      )
    }
  }
  private val head = new AtomicReference[HeadTimestamps](
    HeadTimestamps(
      EffectiveTime(CantonTimestamp.MinValue),
      ApproximateTime(CantonTimestamp.MinValue),
    )
  )

  override def updateHead(
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
      potentialTopologyChange: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    val curHead =
      head.updateAndGet(_.update(effectiveTimestamp, approximateTimestamp))
    // now notify the futures that wait for this update here. as the update is active at t+epsilon, (see most recent timestamp),
    // we'll need to notify accordingly
    notifyAwaitedFutures(curHead.effectiveTimestamp.value.immediateSuccessor)
    if (potentialTopologyChange)
      checkAwaitingConditions()
  }

  protected def currentKnownTime: CantonTimestamp = topologyKnownUntilTimestamp

  override def numPendingChanges: Int = pendingChanges.get()

  protected def observedInternal(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    // we update the head timestamp approximation with the current sequenced timestamp, right now
    updateHead(
      effectiveTimestamp,
      ApproximateTime(sequencedTimestamp.value),
      potentialTopologyChange = false,
    )
    // notify anyone who is waiting on some condition
    checkAwaitingConditions()
    // and we schedule an update to the effective time in due time so that we start using the
    // right keys at the right time.
    if (effectiveTimestamp.value > sequencedTimestamp.value) {
      val deltaDuration = effectiveTimestamp.value - sequencedTimestamp.value
      pendingChanges.incrementAndGet()
      // schedule using after as we don't know the clock synchronisation level, but we know the relative time.
      clock
        .scheduleAfter(
          _ => {
            updateHead(
              effectiveTimestamp,
              ApproximateTime(effectiveTimestamp.value),
              potentialTopologyChange = true,
            )
            if (pendingChanges.decrementAndGet() == 0) {
              logger.debug(
                s"Effective at $effectiveTimestamp, there are no more pending topology changes (last were from $sequencedTimestamp)"
              )
            }
          },
          deltaDuration,
        )
        .discard
    }
    FutureUnlessShutdown.unit
  }

  /** Returns whether a snapshot for the given timestamp is available. */
  override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
    topologyKnownUntilTimestamp >= timestamp

  override def topologyKnownUntilTimestamp: CantonTimestamp =
    head.get().effectiveTimestamp.value.immediateSuccessor

  /** returns the current approximate timestamp
    *
    * whenever we get an update, we do set the approximate timestamp first to the sequencer time
    * and schedule an update on the clock to advance the approximate time to the effective time
    * after the time difference elapsed.
    */
  override def approximateTimestamp: CantonTimestamp =
    head.get().approximateTimestamp.value.immediateSuccessor

  override def awaitTimestampUS(timestamp: CantonTimestamp, waitForEffectiveTime: Boolean)(implicit
      traceContext: TraceContext
  ): Option[FutureUnlessShutdown[Unit]] =
    if (waitForEffectiveTime)
      this.awaitKnownTimestampUS(timestamp)
    else
      Some(
        for {
          snapshotAtTs <- awaitSnapshotUS(timestamp)
          parametersAtTs <- performUnlessClosingF(functionFullName)(
            snapshotAtTs.findDynamicDomainParametersOrDefault(protocolVersion)
          )
          epsilonAtTs = parametersAtTs.topologyChangeDelay
          // then, wait for t+e
          _ <- awaitKnownTimestampUS(timestamp.plus(epsilonAtTs.unwrap))
            .getOrElse(FutureUnlessShutdown.unit)
        } yield ()
      )

  override def awaitTimestamp(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[Future[Unit]] = if (waitForEffectiveTime)
    this.awaitKnownTimestamp(timestamp)
  else if (approximateTimestamp >= timestamp) None
  else {
    Some(
      // first, let's wait until we can determine the epsilon for the given timestamp
      for {
        snapshotAtTs <- awaitSnapshot(timestamp)
        parametersAtTs <- snapshotAtTs.findDynamicDomainParametersOrDefault(protocolVersion)
        epsilonAtTs = parametersAtTs.topologyChangeDelay
        // then, wait for t+e
        _ <- awaitKnownTimestamp(timestamp.plus(epsilonAtTs.unwrap)).getOrElse(Future.unit)
      } yield ()
    )
  }

  override protected def onClosed(): Unit = {
    expireTimeAwaiter()
    super.onClosed()
  }

  override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    scheduleAwait(condition(currentSnapshotApproximation), timeout)

}

object StoreBasedDomainTopologyClient {

  def NoPackageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]] = { _ =>
    EitherT(Future.successful(Either.right(Set.empty[PackageId])))
  }
}
