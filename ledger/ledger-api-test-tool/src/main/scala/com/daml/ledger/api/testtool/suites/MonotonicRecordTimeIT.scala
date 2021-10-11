// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.test.model.Test.Dummy
import scalapb.TimestampConverters

import scala.concurrent.Future

final class MonotonicRecordTimeIT extends LedgerTestSuite {
  import MonotonicRecordTimeIT._

  test(
    "RTMonotonicallyIncreasing",
    "Record Time increases monotonically",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- Future.traverse(1 to Submissions) { _ =>
        ledger.create(party, Dummy(party))
      }
      checkpoints <- ledger.checkpoints(Submissions, ledger.begin)(party)
    } yield {
      assertLength(s"At least $Submissions checkpoints available", Submissions, checkpoints)

      val recordTimes = checkpoints
        .collect { case Checkpoint(Some(recordTime), Some(offset)) => recordTime -> offset }
        .sortBy(_._2.getAbsolute)
        .map { case (recordTime, _) => TimestampConverters.asJavaInstant(recordTime) }
      val wronglySortedRecordTimes = recordTimes
        .zip(recordTimes.sorted)
        .filter { case (produced, sorted) => produced != sorted }
        .map(_._1)
      val prettyPrintedWronglySRecordTimes =
        wronglySortedRecordTimes
          .mkString(
            start = "[",
            sep = ", ",
            end = "]",
          ) // Java instants' "toString" prints them in ISO format
      assert(
        wronglySortedRecordTimes.isEmpty,
        s"some record times are not monotonically increasing: $prettyPrintedWronglySRecordTimes",
      )
    }
  })
}

private object MonotonicRecordTimeIT {
  val Submissions = 100
}
