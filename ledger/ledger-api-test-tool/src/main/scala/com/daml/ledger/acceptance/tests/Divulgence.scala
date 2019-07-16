// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.tests

import com.daml.ledger.acceptance.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.value.Value.Sum.{ContractId, Party}

import scala.concurrent.Future

final class Divulgence(session: LedgerSession) extends LedgerTestSuite(session) {

  private val flatStreamDivulgence =
    LedgerTest("Divulged contracts should not be exposed in the flat transaction stream") {
      implicit context =>
        for {
          Vector(alice, bob) <- allocateParties(2)
          divulgence1 <- create(
            alice,
            templateIds.divulgence1,
            "div1Party" -> new Party(alice)
          )
          divulgence2 <- create(
            bob,
            templateIds.divulgence2,
            "div2Signatory" -> new Party(bob),
            "div2Fetcher" -> new Party(alice)
          )
          _ <- exercise(
            alice,
            templateIds.divulgence2,
            divulgence2,
            "Divulgence2Archive",
            "div1ToArchive" -> new ContractId(divulgence1))
          transactions <- transactionsSinceStart(bob)
        } yield {
          assert(
            transactions.size == 1,
            s"Bob should see exactly one transaction but sees ${transactions.size} instead")
          val events = transactions.head.events
          assert(
            events.size == 1,
            s"The transaction should contain exactly one event but contains ${events.size} instead")
          val event = events.head.event
          assert(event.isCreated, "The transaction should contain a created event")
          val contractId = event.created.get.contractId
          assert(
            contractId == divulgence2,
            "The only visible event should be the creation of the second contract")
        }
    }

  private val acsDivulgence = {
    LedgerTest("Divulged contracts should not be exposed from the active contracts service") {
      implicit context =>
        Future {
          ???
        }
    }
  }

  override val tests: Vector[LedgerTest] = Vector(
    flatStreamDivulgence,
    acsDivulgence
  )

}
