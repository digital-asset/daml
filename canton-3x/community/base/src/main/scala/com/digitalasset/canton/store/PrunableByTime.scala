// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import com.digitalasset.canton.config.RequireTypes.{PositiveDouble, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.store.PrunableByTimeParameters.ControlFactors
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.google.common.annotations.VisibleForTesting

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

/** Various parameter to control prunable by time batching (used for journal pruning)
  *
  * @param targetBatchSize Defines the ideal pruning batch size.  If the batches are larger than the target pruning size, the number of buckets is doubled. If they are substantially smaller than the target pruning size, the number of buckets is reduced by 10%.
  * @param initialInterval The start interval for the prune batching
  * @param maxBuckets maximum number of buckets to split a batch into (limit the iterations when nodes are inactive for quite a while)
  * @param controlFactors The adjustment parameters for the prune batch size computation (threshold, factor)
  */
final case class PrunableByTimeParameters(
    targetBatchSize: PositiveInt,
    initialInterval: NonNegativeFiniteDuration,
    maxBuckets: PositiveInt,
    controlFactors: Seq[ControlFactors] = PrunableByTimeParameters.DefaultControlFactors,
)

object PrunableByTimeParameters {

  /** Prunable by time control parameters
    *
    * When we try to compute the optimal batching parameter, we adjust the batching interval dynamically using
    * these parameters.
    * So a threshold of 4.0 with factor of 0.5 would mean that if the average batch size exceeded 4 times the
    * target, then the interval should be reduced by 50%.
    */
  final case class ControlFactors(
      threshold: PositiveDouble,
      factor: PositiveDouble,
  ) {
    require((threshold.value < 1 && factor.value > 1) || (threshold.value > 1 && factor.value < 1))
  }

  @VisibleForTesting
  lazy val testingParams: PrunableByTimeParameters = PrunableByTimeParameters(
    targetBatchSize = PositiveInt.tryCreate(10),
    initialInterval = NonNegativeFiniteDuration.tryOfSeconds(5),
    maxBuckets = PositiveInt.tryCreate(10),
  )

  private lazy val DefaultControlFactors = Seq(
    ControlFactors(PositiveDouble.tryCreate(4.0), PositiveDouble.tryCreate(0.5)),
    ControlFactors(PositiveDouble.tryCreate(0.2), PositiveDouble.tryCreate(1.1)),
  )
}

/** Interface for a store that allows pruning and keeps track of when pruning has started and finished. */
trait PrunableByTime {

  protected implicit val ec: ExecutionContext
  protected def kind: String

  /** Parameters to control prune batching
    *
    * If defined, then the pruning window will be computed such that it targets
    * the ideal target batch size in order to optimize the load on the database.
    *
    * This is currently used with the journal stores. Normal pruning of other stores already does batching on its own.
    */
  protected def batchingParameters: Option[PrunableByTimeParameters] = None

  /** Prune all unnecessary data relating to events before the given timestamp.
    *
    * The meaning of "unnecessary", and whether the limit is inclusive or exclusive both depend on the particular store.
    * The store must implement the actual pruning logic in the [[doPrune]] method.
    */
  final def prune(
      limit: CantonTimestamp
  )(implicit errorLoggingContext: ErrorLoggingContext, closeContext: CloseContext): Future[Unit] = {
    implicit val traceContext: TraceContext = errorLoggingContext.traceContext
    val ret = for {
      lastTs <- FutureUnlessShutdown.outcomeF(getLastPruningTs)
      _ <- FutureUnlessShutdown.outcomeF(advancePruningTimestamp(PruningPhase.Started, limit))
      // prune in buckets to avoid generating too large db transactions
      res <- MonadUtil.sequentialTraverse(
        computeBuckets(lastTs, limit)
      ) { case (prev, next) =>
        closeContext.context.performUnlessClosingF(s"prune interval ${next}")(doPrune(next, prev))
      }
      _ <- FutureUnlessShutdown.outcomeF(advancePruningTimestamp(PruningPhase.Completed, limit))
    } yield {
      val num = res.sum
      if (num > 0)
        errorLoggingContext.logger.debug(s"Pruned $num $kind using ${res.length} intervals")
      lastTs.foreach(ts => updateBucketSize(res, limit - ts))
    }
    ret.onShutdown(())
  }

  private val stepSizeMillis = new AtomicReference[Long](
    batchingParameters
      .map(_.initialInterval)
      .getOrElse(NonNegativeFiniteDuration.tryOfSeconds(60))
      .duration
      .toMillis
  )

  private def updateBucketSize(res: Seq[Int], elapsed: Duration)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Unit =
    (res.nonEmpty, batchingParameters) match {
      case (true, Some(parameters)) =>
        // if the average number of items deleted in the batch exceeds the target, we double the number of buckets
        val average = res.sum.toDouble / res.length
        // find the first factor that matches the definition
        val factorForUpdate = parameters.controlFactors.find { case ControlFactors(threshold, _) =>
          (threshold.value > 1 && average > parameters.targetBatchSize.value * threshold.value) || (threshold.value < 1 && average < parameters.targetBatchSize.value * threshold.value)
        }
        factorForUpdate match {
          // adjust the interval size
          case Some(ControlFactors(_, factor)) =>
            val old = stepSizeMillis.get()
            val cur = stepSizeMillis.updateAndGet { current =>
              val updated = Math.max(1, Math.round(current * factor.value))
              // limit interval increase if we are already restrained
              if (
                // do not lower the interval if we are already at max buckets
                (factor.value < 1.0 && (elapsed.toMillis / current) < parameters.maxBuckets.value) ||
                // do not increase interval beyond the actual pruning frequency
                // example: if we are pruning every 60s and we have so few rows that we only need one batch, we don't
                // want the step size to keep on growing to infinity, so we cap it roughly at the reconciliation interval.
                (factor.value > 1.0 && current <= elapsed.toMillis * factor.value)
              ) {
                updated
              } else current
            }
            if (old != cur) {
              errorLoggingContext.logger.debug(
                s"Updating pruning interval of ${kind} to ${cur} ms for average ${average.toInt} in ${res.length} buckets"
              )(
                errorLoggingContext.traceContext
              )
            }
          case None =>
        }
      case _ =>
    }

  /** Compute the pruning intervals to invoke pruning on
    *
    * If lastTsO is set, then an overlapping sequence of intervals is returned into
    * which the pruning should be bucketed.
    */
  private def computeBuckets(
      lastTsO: Option[CantonTimestamp],
      limit: CantonTimestamp,
  ): Seq[(Option[CantonTimestamp], CantonTimestamp)] = {
    (lastTsO, batchingParameters) match {
      case (Some(lastTs), Some(parameters)) if parameters.maxBuckets.value > 1 =>
        // limit batching to "max num buckets" such that unsteady load throughout the day doesn't lead to
        // thousands of empty batches being sent
        val stepMs =
          Math.max(stepSizeMillis.get(), (limit - lastTs).toMillis / parameters.maxBuckets.value)
        @tailrec
        def go(
            lower: CantonTimestamp,
            accumulate: Seq[(Option[CantonTimestamp], CantonTimestamp)],
        ): Seq[(Option[CantonTimestamp], CantonTimestamp)] = {
          val upper = lower.plusMillis(stepMs)
          // prepend for efficiency (we reverse below)
          if (upper >= limit) (Some(lower), limit) +: accumulate
          else go(upper, (Some(lower), upper) +: accumulate)
        }
        go(lastTs, Seq.empty).reverse
      case _ => Seq((lastTsO, limit))
    }
  }
  private def getLastPruningTs(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = pruningStatus.map(_.flatMap(_.lastSuccess))

  /** Returns the latest timestamp at which pruning was started or completed.
    * For [[com.digitalasset.canton.pruning.PruningPhase.Started]], it is guaranteed
    * that no pruning has been run on the store after the returned timestamp.
    * For [[com.digitalasset.canton.pruning.PruningPhase.Completed]], it is guaranteed
    * that the store is pruned at least up to the returned timestamp (inclusive).
    * That is, another pruning with the returned timestamp (or earlier) has no effect on the store.
    * Returns [[scala.None$]] if no pruning has ever been started on the store.
    */
  def pruningStatus(implicit traceContext: TraceContext): Future[Option[PruningStatus]]

  @VisibleForTesting
  protected[canton] def advancePruningTimestamp(phase: PruningPhase, timestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): Future[Unit]

  /** Actual invocation of doPrune
    *
    * @return the approximate number of pruned rows, used to adjust the pruning windows to reach optimal batch sizes
    */
  @VisibleForTesting
  protected[canton] def doPrune(limit: CantonTimestamp, lastPruning: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): Future[Int]

}
