// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsProvider.ComputeReconciliationIntervalsCoveringIterable
import com.digitalasset.canton.protocol.SynchronizerParameters
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherUtil.*

import java.security.InvalidParameterException
import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.util.chaining.*

class SortedReconciliationIntervalsProvider(
    topologyClient: SynchronizerTopologyClient,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private val approximateLatestReconciliationInterval = new AtomicReference[
    Option[SortedReconciliationIntervals.ReconciliationInterval]
  ](None)

  def getApproximateLatestReconciliationInterval
      : Option[SortedReconciliationIntervals.ReconciliationInterval] =
    approximateLatestReconciliationInterval.get()

  private def getAll(validAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[SynchronizerParameters.WithValidity[PositiveSeconds]]] =
    futureSupervisor
      .supervisedUS(s"Querying for list of synchronizer parameters changes valid at $validAt") {
        topologyClient.awaitSnapshot(validAt)
      }
      .flatMap(snapshot => snapshot.listDynamicSynchronizerParametersChanges())
      .map(_.map(_.map(_.reconciliationInterval)))

  def reconciliationIntervals(
      validAt: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SortedReconciliationIntervals] =
    getAll(validAt)
      .map { reconciliationIntervals =>
        SortedReconciliationIntervals
          .create(
            reconciliationIntervals,
            validUntil = validAt,
          )
          .tapLeft(logger.error(_))
          .getOrElse(SortedReconciliationIntervals.empty)
          .tap { sortedReconciliationIntervals =>
            sortedReconciliationIntervals.intervals.headOption.foreach { latest =>
              approximateLatestReconciliationInterval.getAndUpdate {
                case None => Some(latest)
                case old @ Some(current) =>
                  if (latest.validFrom > current.validFrom) Some(latest) else old
              }.discard
            }
          }
      }

  /** checks if the given timestamp `ts` represents a reconciliation interval tick returns an
    * Option[Boolean] if returned is None then the given `ts` is older than the oldest
    * reconciliation interval otherwise returns a boolean stating if it is a tick or not
    */
  private def validateTick(
      ts: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[Boolean]] =
    for {
      sortedReconciliationIntervals <- reconciliationIntervals(ts)
      isTick = sortedReconciliationIntervals.isAtTick(ts)
    } yield isTick

  /** Succeeds if the given timestamp `ts` represents a reconciliation interval tick otherwise
    * throws an InvalidParameterException.
    */
  private def tryCheckIsTick(
      ts: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      isTick <- validateTick(ts)
    } yield {
      isTick match {
        case Some(value) =>
          if (!value) throw new InvalidParameterException(s"$ts is not a valid tick")
        case None =>
          throw new InvalidParameterException(
            s"Cannot tell whether $ts is a tick, because the query uses an older reconciliation interval"
          )
      }
    }

  /** returns true if the given timestamp `ts` represents a reconciliation interval tick otherwise
    * returns false.
    */
  private def isTick(
      ts: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    for {
      isTick <- validateTick(ts)
    } yield {
      isTick match {
        case Some(value) =>
          if (!value) {
            false
          } else true
        case None =>
          false
      }
    }

  /** splits a given CommitmentPeriod into a NonEmpty iterable of CommitmentPeriods with ValidTicks.
    * if the given period does not have any valid periods within it then None is returned.
    *
    * if given period( from = 0, to = 100) and we have one reconciliationInterval of 5 then it will
    * return a NonEmpty set with 20 periods (0 -> 5, 5 -> 10, 10 -> 15 etc.)
    *
    * if multiple reconciliation intervals exists then it gets split correctly as well.
    *
    * if given period( from = 0, to 100) and we have two reconciliationIntervals (5 at 0 and 10 at
    * 30) then it will return a NonEmpty iterable with 6 + 7 periods (0 -> 5, 5 -> 10, 10 -> 15
    * [...] 30
    * -> 40, 40 -> 50, 50 -> 60 etc.)
    */

  def splitCommitmentPeriod(commitmentPeriod: CommitmentPeriod)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[NonEmpty[immutable.Iterable[CommitmentPeriod]]]] =
    for {
      isFromTick <- isTick(commitmentPeriod.fromExclusive.forgetRefinement)
      isToTick <- isTick(commitmentPeriod.toInclusive.forgetRefinement)
      periods <-
        if (isFromTick & isToTick)
          computeReconciliationIntervalsCovering(
            commitmentPeriod.fromExclusive.forgetRefinement,
            commitmentPeriod.toInclusive.forgetRefinement,
          )
        else
          FutureUnlessShutdown.pure(List.empty)
    } yield NonEmpty.from(periods)

  /** Computes a list of commitment periods between `fromExclusive` to `toInclusive`. The caller
    * should ensure that `fromExclusive` and `toInclusive` represent valid reconciliation ticks.
    * Otherwise, the method throws an `InvalidParameterException`
    */
  def computeReconciliationIntervalsCovering(
      fromExclusive: CantonTimestamp,
      toInclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[CommitmentPeriod]] =
    for {
      _ <- tryCheckIsTick(fromExclusive)
      _ <- tryCheckIsTick(toInclusive)
      sortedReconciliationIntervals <- reconciliationIntervals(toInclusive)
    } yield new ComputeReconciliationIntervalsCoveringIterable(
      sortedReconciliationIntervals,
      CantonTimestampSecond.floor(fromExclusive),
      CantonTimestampSecond.floor(toInclusive),
    )
}

object SortedReconciliationIntervalsProvider {
  private class ComputeReconciliationIntervalsCoveringIterable(
      sortedReconciliationIntervals: SortedReconciliationIntervals,
      fromExclusive: CantonTimestampSecond,
      toInclusive: CantonTimestampSecond,
  ) extends immutable.Iterable[CommitmentPeriod] {
    import ComputeReconciliationIntervalsCoveringIterable.*

    private type State = CantonTimestampSecond

    override def iterator: Iterator[CommitmentPeriod] =
      Iterator.unfold(toInclusive)(next)

    private def next(tick: CantonTimestampSecond): Option[(CommitmentPeriod, State)] =
      if (tick <= fromExclusive) None
      else {
        val tickBefore = sortedReconciliationIntervals.tickBefore(tick.forgetRefinement)
        val lastPeriod = tickBefore match {
          case Some(tickBef) if tickBef > fromExclusive =>
            CommitmentPeriod(tickBef, periodLength(tickBef, tick))
          case _ =>
            CommitmentPeriod(fromExclusive, periodLength(fromExclusive, tick))
        }
        val nextTick = lastPeriod.fromExclusive
        Some(lastPeriod -> nextTick)
      }
  }

  private object ComputeReconciliationIntervalsCoveringIterable {
    private def periodLength(
        start: CantonTimestampSecond,
        end: CantonTimestampSecond,
    ): PositiveSeconds =
      PositiveSeconds.tryOfSeconds(end.getEpochSecond - start.getEpochSecond)
  }

}
