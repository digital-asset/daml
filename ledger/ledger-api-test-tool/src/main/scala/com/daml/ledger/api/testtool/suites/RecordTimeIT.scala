// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import scalapb.TimestampConverters

import scala.concurrent.Future
import scala.util.Random

final class RecordTimeIT extends LedgerTestSuite {

  test(
    "RecordTimeMonotonicallyIncreasing",
    "Record Time increases monotonically",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val submissions = 50
    for {
      _ <- Future.traverse(1 to submissions) { number =>
        ledger.allocateParty(
          partyIdHint = Some(
            s"recordTimeMonotonicallyIncreasing_${number}_" + Random.alphanumeric.take(100).mkString
          ),
          displayName = Some(s"Clone $number"),
        )
      }
      checkpoints <- ledger.checkpoints(submissions, ledger.begin)(party)
    } yield {
      val recordTimes = checkpoints
        .flatMap(_.recordTime.toList)
        .map(TimestampConverters.asJavaInstant)
      assertLength("As many record times as submissions", submissions, recordTimes)
      val unsorted = recordTimes
        .zip(recordTimes.sorted)
        .filter { case (produced, sorted) => produced != sorted }
        .map(_._1)
      assert(
        unsorted.isEmpty,
        s"some record times are not monotonically increasing: $unsorted", // Instants will be printed in instant ISO format
      )
    }
  })
}
