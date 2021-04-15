// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.test.model.Test.DummyWithAnnotation

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random

final class ValueLimitsIT(timeoutScaleFactor: Double) extends LedgerTestSuite {

  /** Postgres has a limit on the index row size of 2712.
    * We test that a large submitters number doesn't cause deduplication failure because of that limit.
    * THIS TEST CASE DOES NOT TEST ANY OTHER FAILURES THAT MAY BE CAUSED BY A LARGE SUBMITTERS NUMBER
    */
  test(
    "VLDeduplicateWithLargeSubmittersNumber",
    "Deduplicate commands with a large number of submitters",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      // Need to manually allocate parties to avoid db string compression
      parties <- Future.traverse(1 to 50) { number =>
        ledger.allocateParty(
          partyIdHint =
            Some(s"deduplicationRandomParty_${number}_" + Random.alphanumeric.take(100).mkString),
          displayName = Some(s"Clone $number"),
        )
      }
      request = ledger
        .submitRequest(
          actAs = parties.toList,
          readAs = parties.toList,
          commands = DummyWithAnnotation(parties.head, "First submission").create.command,
        )
        .update(
          _.commands.deduplicationTime := deduplicationTime.asProtobuf
        )
      _ <- ledger.submit(request)
    } yield {
      ()
    }
  })

  private val deduplicationTime = 3.seconds * timeoutScaleFactor match {
    case duration: FiniteDuration => duration
    case _ =>
      throw new IllegalArgumentException(s"Invalid timeout scale factor: $timeoutScaleFactor")
  }

}
