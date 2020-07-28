// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.tests.TransactionScaleIT.numberOfCommandsUnit
import com.daml.ledger.test_stable.Test.{Dummy, TextContainer}

import scala.concurrent.Future

class TransactionScaleIT(loadScaleFactor: Double) extends LedgerTestSuite {
  require(
    numberOfCommands(units = 1) > 0,
    s"The load scale factor must be at least ${1.0 / numberOfCommandsUnit}",
  )

  test(
    "TXLargeCommand",
    "Accept huge submissions with a large number of commands",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, party)) =>
      val targetNumberOfSubCommands = numberOfCommands(units = 3)
      val request = ledger.submitAndWaitRequest(
        party,
        List.fill(targetNumberOfSubCommands)(Dummy(party).create.command): _*,
      )
      for {
        result <- ledger.submitAndWaitForTransaction(request)
      } yield {
        val _ = assertLength("LargeCommand", targetNumberOfSubCommands, result.events)
      }
  })

  test("TXManyCommands", "Accept many, large commands at once", allocate(SingleParty))(
    implicit ec => {
      case Participants(Participant(ledger, party)) =>
        val targetNumberOfCommands = numberOfCommands(units = 1)
        val oneKbOfText = new String(Array.fill(512 /* two bytes each */ )('a'))
        for {
          contractIds <- Future.sequence(
            (1 to targetNumberOfCommands).map(_ =>
              ledger.create(party, TextContainer(party, oneKbOfText))),
          )
        } yield {
          val _ = assertLength("ManyCommands", targetNumberOfCommands, contractIds)
        }
    })

  private def numberOfCommands(units: Int): Int =
    (units * numberOfCommandsUnit * loadScaleFactor).toInt

}

object TransactionScaleIT {
  private val numberOfCommandsUnit = 500
}
