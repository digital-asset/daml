// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import slick.jdbc.{GetResult, SetParameter}

import scala.math.Ordering.Implicits.infixOrderingOps

/** The time when a request or a repair has changed the state at a specific participant within a
  * synchronizer. The TimeOfChange augments a sequencer request timestamp with an optional repair
  * counter lexicographically relative to the timestamp.
  *
  * @param timestamp
  *   The sequenced request timestamp assigned by phase 3 of the canton protocol or the timestamp of
  *   the previous request in case of a repair request.
  * @param counterO
  *   The relative repair counter in case of repair request.
  */
final case class TimeOfChange private (
    timestamp: CantonTimestamp,
    counterO: Option[RepairCounter],
) extends PrettyPrinting {
  def toDbPrimitive: (CantonTimestamp, RepairCounter) =
    (timestamp, counterO.getOrElse(RepairCounter.MinValue))

  override protected def pretty: Pretty[TimeOfChange] = prettyOfClass(
    param("timestamp", _.timestamp),
    paramIfNonEmpty("counter", _.counterO),
  )
}

object TimeOfChange {

  /** Builds a time of change in non-repair cases such as conflict detection/request tracking and
    * timestamp-based boundaries such as pruning and ACS commitments.
    */
  def apply(ts: CantonTimestamp, counterO: Option[RepairCounter]): TimeOfChange = {
    counterO.foreach { counter =>
      if (counter.unwrap < 0L) {
        throw new IllegalStateException(s"RepairCounter must be non-negative, but was $counter")
      }
    }
    new TimeOfChange(ts, counterO)
  }

  /** Builds a time of change in non-repair cases such as conflict detection/request tracking and
    * timestamp-based boundaries such as pruning and ACS commitments.
    */
  def apply(ts: CantonTimestamp): TimeOfChange = TimeOfChange(ts, None)

  /** Builds a time of change mapping negative repair counters to None for non-nullable db and
    * conversion from AcsCommitmentProcessor RecordTime.
    */
  def negativeCounterToNone(ts: CantonTimestamp, counter: RepairCounter): TimeOfChange =
    TimeOfChange(ts, Option.when(counter.unwrap >= 0)(counter))

  implicit val orderingTimeOfChange: Ordering[TimeOfChange] =
    Ordering.by[TimeOfChange, (CantonTimestamp, Option[RepairCounter])](toc =>
      (toc.timestamp, toc.counterO)
    )

  implicit val getResultTimeOfChange: GetResult[TimeOfChange] = GetResult { r =>
    val ts = r.<<[CantonTimestamp]
    val counter = r.<<[RepairCounter]
    TimeOfChange.negativeCounterToNone(ts, counter)
  }

  implicit val setParameterTimeOfChange: SetParameter[TimeOfChange] = (v, pp) =>
    withMinAsNoneRepairCounter(v) { case (ts, rc) =>
      pp >> ts
      pp >> rc
    }

  /** Helper to map a None repairCounter to RepairCounter.MinValue for non-nullable db persistence
    * to reflect the fact that a non-repair request at a timestamp is ordered before repair requests
    * with the same timestamp.
    */
  def withMinAsNoneRepairCounter[T](toc: TimeOfChange)(
      f: (CantonTimestamp, RepairCounter) => T
  ): T = {
    val (ts, repairCounter) = toc.toDbPrimitive
    f(ts, repairCounter)
  }

  /** TimeOfChange that includes all the repairs up to the given timestamp exclusively. Useful for
    * obtaining ACS snapshots prior to the given timestamp.
    */
  def immediatePredecessor(tsUpToExclusive: CantonTimestamp): TimeOfChange =
    TimeOfChange(tsUpToExclusive.immediatePredecessor, Some(RepairCounter.MaxValue))

  /** Helper to obtain TimeOfChange from the indexer synchronizer index using the maximum of the
    * record time and the repair index.
    */
  def fromSynchronizerIndex(synchronizerIndex: SynchronizerIndex): TimeOfChange = {
    val timeOfChangeRecordTime = TimeOfChange(synchronizerIndex.recordTime, None)
    synchronizerIndex.repairIndex.fold(timeOfChangeRecordTime)(repairIndex =>
      timeOfChangeRecordTime max TimeOfChange(
        repairIndex.timestamp,
        Some(repairIndex.counter),
      )
    )
  }
}
