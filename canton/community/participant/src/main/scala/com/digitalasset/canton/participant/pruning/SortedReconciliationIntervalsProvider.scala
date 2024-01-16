// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.protocol.{DomainParametersLookup, StaticDomainParameters}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherUtil.*
import com.google.common.annotations.VisibleForTesting

import java.security.InvalidParameterException
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*

class SortedReconciliationIntervalsProvider(
    reconciliationIntervalsProvider: DomainParametersLookup[PositiveSeconds],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private val approximateLatestReconciliationInterval = new AtomicReference[
    Option[SortedReconciliationIntervals.ReconciliationInterval]
  ](None)

  def getApproximateLatestReconciliationInterval
      : Option[SortedReconciliationIntervals.ReconciliationInterval] =
    approximateLatestReconciliationInterval.get()

  def approximateReconciliationIntervals(implicit
      traceContext: TraceContext
  ): Future[SortedReconciliationIntervals] = reconciliationIntervals(
    reconciliationIntervalsProvider.approximateTimestamp
  )

  def reconciliationIntervals(
      validAt: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): Future[SortedReconciliationIntervals] =
    reconciliationIntervalsProvider
      .getAll(validAt)
      .map { reconciliationIntervals =>
        SortedReconciliationIntervals
          .create(
            reconciliationIntervals,
            validUntil = validAt,
          )
          .tapLeft(logger.error(_))
          .getOrElse(SortedReconciliationIntervals.empty)
          .tap { sortedReconciliationIntervals =>
            val latest = sortedReconciliationIntervals.intervals.headOption

            approximateLatestReconciliationInterval.set(latest)
          }
      }

  /** Succeeds if the given timestamp `ts` represents a reconciliation interval tick,
    * otherwise throws an InvalidParameterException.
    */
  private def checkIsTick(
      ts: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = {
    for {
      sortedReconciliationIntervals <- reconciliationIntervals(ts)
      isTick = sortedReconciliationIntervals.isAtTick(ts)
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
  }

  /** Computes a list of commitment periods between `fromExclusive` to `toInclusive`.
    * The caller should ensure that `fromExclusive` and `toInclusive` represent valid reconciliation ticks.
    * Otherwise, the method throws an `InvalidParameterException`
    */
  @VisibleForTesting
  private[pruning] def computeReconciliationIntervalsCovering(
      fromExclusive: CantonTimestamp,
      toInclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[List[CommitmentPeriod]] = {

    for {
      _ <- checkIsTick(fromExclusive)
      _ <- checkIsTick(toInclusive)
      res <-
        if (fromExclusive.getEpochSecond >= toInclusive.getEpochSecond)
          Future.successful(List.empty[CommitmentPeriod])
        else
          for {
            sortedReconciliationIntervals <- reconciliationIntervals(toInclusive)
            tickBefore = sortedReconciliationIntervals.tickBefore(toInclusive)
            wholePeriod = CommitmentPeriod(
              CantonTimestampSecond.ofEpochSecond(fromExclusive.getEpochSecond),
              PositiveSeconds.tryOfSeconds((toInclusive - fromExclusive).getSeconds),
            )
            lastInterval = tickBefore match {
              case Some(tick) =>
                if (tick > fromExclusive)
                  CommitmentPeriod(
                    CantonTimestampSecond.ofEpochSecond(tick.getEpochSecond),
                    PositiveSeconds.tryOfSeconds(
                      toInclusive.getEpochSecond - tick.getEpochSecond
                    ),
                  )
                else wholePeriod
              case None => wholePeriod
            }
            prevIntervals <- computeReconciliationIntervalsCovering(
              fromExclusive,
              lastInterval.fromExclusive.forgetRefinement,
            )
          } yield {
            prevIntervals :+ lastInterval
          }
    } yield res
  }
}

object SortedReconciliationIntervalsProvider {
  def apply(
      staticDomainParameters: StaticDomainParameters,
      topologyClient: DomainTopologyClient,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): SortedReconciliationIntervalsProvider =
    new SortedReconciliationIntervalsProvider(
      DomainParametersLookup.forReconciliationInterval(
        staticDomainParameters,
        topologyClient,
        futureSupervisor,
        loggerFactory,
      ),
      loggerFactory,
    )
}
