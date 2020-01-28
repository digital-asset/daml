// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.test_stable.Test.{Dummy, TextContainer}
import TransactionScale.numberOfCommandsUnit

import scala.concurrent.Future

class TransactionScale(session: LedgerSession) extends LedgerTestSuite(session) {
  require(
    numberOfCommands(units = 1) > 0,
    s"The load scale factor must be at least ${1.0 / numberOfCommandsUnit}",
  )

  test(
    "TXLargeCommand",
    "Accept huge submissions with a large number of commands",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      val targetNumberOfSubCommands = numberOfCommands(units = 3)
      for {
        request <- ledger.submitAndWaitRequest(
          party,
          List.fill(targetNumberOfSubCommands)(Dummy(party).create.command): _*,
        )
        result <- ledger.submitAndWaitForTransaction(request)
      } yield {
        val _ = assertLength("LargeCommand", targetNumberOfSubCommands, result.events)
      }
  }

  test("TXManyCommands", "Accept many, large commands at once", allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      val targetNumberOfCommands = numberOfCommands(units = 1)
      val oneKbOfText = new String(Array.fill(512 /* two bytes each */ )('a'))
      for {
        contractIds <- Future.sequence(
          (1 to targetNumberOfCommands).map(_ =>
            ledger.create(party, TextContainer(party, oneKbOfText)),
          ),
        )
      } yield {
        val _ = assertLength("ManyCommands", targetNumberOfCommands, contractIds)
      }
  }

  private def numberOfCommands(units: Int): Int =
    (units * numberOfCommandsUnit * session.config.loadScaleFactor).toInt

}

object TransactionScale {
  private val numberOfCommandsUnit = 500
}
