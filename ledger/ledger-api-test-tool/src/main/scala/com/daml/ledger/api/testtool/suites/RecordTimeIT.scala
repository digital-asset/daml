// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.google.protobuf.timestamp.{Timestamp => ProtoTimestamp}
import scalapb.TimestampConverters

import scala.concurrent.Future
import scala.util.Random

final class RecordTimeIT extends LedgerTestSuite {

  test(
    "RecordTimeMonotonicallyIncreasing",
    "Record Time increases monotonically",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val operations = 50
    for {
      _ <- Future.traverse(1 to operations) { number =>
        ledger.allocateParty(
          partyIdHint = Some(
            s"recordTimeMonotonicallyIncreasing_${number}_" + Random.alphanumeric.take(100).mkString
          ),
          displayName = Some(s"Clone $number"),
        )
      }
      checkpoints <- ledger.checkpoints(operations, ledger.begin)()
      recordTimes = checkpoints.flatMap(_.recordTime)
    } yield {
      val monotonicallyIncreasing = recordTimes
        .foldLeft((true, None: Option[ProtoTimestamp])) {
          case (false, _) -> _ => (false, None)
          case (true, None) -> firstTimestamp => (true, Some(firstTimestamp))
          case (true, Some(previousTimestamp)) -> newTimestamp =>
            TimestampConverters
              .asJavaInstant(previousTimestamp)
              .isBefore(TimestampConverters.asJavaInstant(newTimestamp)) ->
              Some(newTimestamp)
        }
        ._1
      assert(monotonicallyIncreasing, "record time is not monotonically increasing")
    }
  })
}
