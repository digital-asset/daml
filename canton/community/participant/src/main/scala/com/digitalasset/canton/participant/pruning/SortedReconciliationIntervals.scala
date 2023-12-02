// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervals.ReconciliationInterval
import com.digitalasset.canton.protocol.DomainParameters
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.time.PositiveSeconds

import java.time.temporal.ChronoUnit
import scala.annotation.tailrec
import scala.math.Ordering.Implicits.*

/** Reconciliation intervals, with their validity intervals, sorted by
  * validFrom in decreasing order.
  * Note: the factory method ensures that the intervals are pairwise disjoint.
  *
  * @param intervals Sorted intervals
  * @param validUntil The data contained in the intervals is valid only
  *                   for timestamps <= `validUntil`.
  */
final case class SortedReconciliationIntervals private (
    intervals: List[ReconciliationInterval],
    validUntil: CantonTimestamp,
) {

  /** Check whether `ts` is on a commitment tick.
    * @return [[scala.None$]] if `ts > validUntil`
    * Note: [[com.digitalasset.canton.data.CantonTimestamp.MinValue]] is always considered to fall on a tick.
    */
  def isAtTick(ts: CantonTimestamp): Option[Boolean] = CantonTimestampSecond
    .fromCantonTimestamp(ts) match {
    case Left(_) => Some(false)
    case Right(ts) => isAtTick(ts)
  }

  /** Check whether `ts` is on a commitment tick.
    * @return [[scala.None$]] if `ts > validUntil`
    * Note: [[com.digitalasset.canton.data.CantonTimestampSecond.MinValue]] is always considered to fall on a tick.
    */
  def isAtTick(ts: CantonTimestampSecond): Option[Boolean] =
    if (ts == CantonTimestampSecond.MinValue) Some(true)
    else if (ts > validUntil)
      None
    else
      intervals
        .collectFirst {
          case interval if interval.isValidAt(ts.forgetRefinement) =>
            val secondsSinceEpoch = ts.getEpochSecond
            secondsSinceEpoch % interval.intervalLength.duration.getSeconds == 0
        }
        .orElse(Some(false))

  /** Returns the latest tick which is <= ts
    * @return [[scala.None$]] if `ts > validUntil`
    * If we query for inside a gap (when no domain parameters are valid), previous ones are used.
    */
  def tickBeforeOrAt(
      ts: CantonTimestamp
  ): Option[CantonTimestampSecond] =
    if (ts == CantonTimestamp.MinValue)
      Some(CantonTimestampSecond.MinValue)
    else if (ts > validUntil)
      None
    else {

      @tailrec
      def go(
          reconciliationIntervals: List[ReconciliationInterval]
      ): Option[CantonTimestampSecond] = reconciliationIntervals match {
        case interval :: tail =>
          if (interval.validFrom <= ts) {
            // The min allows to project back if there is a gap in the validity intervals
            val currentTs = interval.validUntil.map(_.min(ts)).getOrElse(ts)

            val tick = tickBeforeOrAtForIntervalLength(currentTs, interval.intervalLength)

            if (interval.isValidAt(tick.forgetRefinement)) Some(tick) else go(tail)
          } else go(tail)

        case Nil => None
      }

      go(intervals).orElse(Some(CantonTimestampSecond.MinValue))
    }

  /** Returns the latest tick which is < ts
    * @return [[scala.None$]] if `ts > validUntil` or if `ts = CantonTimestamp.MinValue`
    * If we query for inside a gap (when no domain parameters are valid), previous ones are used.
    */
  def tickBefore(ts: CantonTimestamp): Option[CantonTimestampSecond] =
    if (ts == CantonTimestamp.MinValue || ts > validUntil)
      None
    else
      tickBeforeOrAt(ts.immediatePredecessor)

  /** Returns the commitment period with endpoints (start, end) that satisfy:
    *
    *   - `start < end <= periodEnd`
    *   - `start = endOfPreviousPeriod | CantonTimestamp.MinValue`
    *
    * If no such period exists (e.g., because `periodEnd <= endOfPreviousPeriod`), None is returned.
    */
  def commitmentPeriodPreceding(
      periodEnd: CantonTimestampSecond,
      endOfPreviousPeriod: Option[CantonTimestampSecond],
  ): Option[CommitmentPeriod] = {
    val periodStart = endOfPreviousPeriod.getOrElse(CantonTimestampSecond.MinValue)
    val periodLength = PositiveSeconds.between(periodStart, periodEnd)

    periodLength.toOption.map(CommitmentPeriod(periodStart, _))
  }

  /** Check whether the given interval contains a tick */
  def containsTick(fromExclusive: CantonTimestamp, toInclusive: CantonTimestamp): Option[Boolean] =
    tickBeforeOrAt(toInclusive).map(fromExclusive < _)

  /** Returns the tick before or at the specified timestamp `ts` considering the
    * reconciliation interval `intervalLength`.
    * Unlike other methods, this one considers the case where `intervalLength` is
    * valid at each point in time.
    */
  private def tickBeforeOrAtForIntervalLength(
      ts: CantonTimestamp,
      intervalLength: PositiveSeconds,
  ): CantonTimestampSecond = {
    // Uses the assumption that the interval has a round number of seconds
    val mod = ts.getEpochSecond % intervalLength.unwrap.getSeconds
    val sinceTickStart = if (mod >= 0) mod else intervalLength.unwrap.getSeconds + mod
    val beforeTs = ts.toInstant.truncatedTo(ChronoUnit.SECONDS).minusSeconds(sinceTickStart)
    CantonTimestampSecond.assertFromInstant(beforeTs.max(CantonTimestamp.MinValue.toInstant))
  }
}

object SortedReconciliationIntervals {
  val empty: SortedReconciliationIntervals =
    SortedReconciliationIntervals(Nil, CantonTimestamp.MinValue)

  /*
  A single reconciliation interval defines tick starting from epoch.
  This way, we have a single point of origin for the computation.
  For example, take two intervals, one with a duration of 3 minutes, the other 10 minutes.
  The first is valid until time = 49.
  Per interval, the ticks to publish commitments are defined as follows:
  1st: epoch, ..., -9, -6, -3, 0, 3, 6, 9
  2st: epoch, ..., -10, 0, 10, 20, 30, 40, 50, 60
  To obtain the final list of ticks, we intersect the ticks with the validity of the domain parameters:
  epoch, .., -9, -6, -3, 0, 3, 6, 9, ..., 45, 48, 50, 60, ...
   */
  final case class ReconciliationInterval(
      validFrom: CantonTimestamp,
      validUntil: Option[CantonTimestamp],
      intervalLength: PositiveSeconds,
  ) {
    def isValidAt(ts: CantonTimestamp) = validFrom < ts && validUntil.forall(ts <= _)
  }

  def create(
      reconciliationIntervals: Seq[DomainParameters.WithValidity[PositiveSeconds]],
      validUntil: CantonTimestamp,
  ): Either[String, SortedReconciliationIntervals] = {
    val sortedReconciliationIntervals =
      reconciliationIntervals.sortBy(_.validFrom)(implicitly[Ordering[CantonTimestamp]].reverse)

    val overlappingValidityIntervals =
      sortedReconciliationIntervals
        .sliding(2)
        .collectFirst { // If validUntil is None it means no end validity, so it should be the first interval
          case Seq(p1, p2) if p2.validUntil.forall(_ > p1.validFrom) => Some((p1, p2))
        }
        .flatten

    overlappingValidityIntervals match {
      case Some((p1, p2)) =>
        Left(s"The list of domain parameters contains overlapping validity intervals: $p1, $p2")

      case None =>
        val intervals = sortedReconciliationIntervals.map {
          case DomainParameters.WithValidity(validFrom, validUntil, reconciliationInterval) =>
            ReconciliationInterval(validFrom, validUntil, reconciliationInterval)
        }.toList

        Right(SortedReconciliationIntervals(intervals, validUntil))
    }
  }
}
