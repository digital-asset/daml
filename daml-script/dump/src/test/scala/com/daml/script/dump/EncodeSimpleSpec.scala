// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeSimpleSpec extends AnyFreeSpec with Matchers {
  import Encode._
  "encodeSubmitSimpleEvents" - {
    "mixed create and exercise events" in {
      val parties = Map(
        Party("Alice") -> "alice_0",
        Party("Bob") -> "bob_0",
      )
      val cidMap = Map(
        ContractId("cid1") -> "contract_0_0",
        ContractId("cid2") -> "contract_0_1",
        ContractId("cid3") -> "contract_0_2",
      )
      val cidRefs = Set(ContractId("cid1"), ContractId("cid3"))
      val events = TestData
        .Tree(
          Seq[TestData.Event](
            TestData.Created(ContractId("cid1"), submitters = Seq(Party("Alice"))),
            TestData.Exercised(
              ContractId("cid2"),
              Seq(
                TestData.Created(ContractId("cid3"))
              ),
              exerciseResult = Some(ContractId("cid3")),
              actingParties = Seq(Party("Bob")),
            ),
          )
        )
        .toSimpleEvents
      encodeSubmitSimpleEvents(parties, cidMap, cidRefs, events).render(80) shouldBe
        """(contract_0_0, contract_0_2) <- submitMulti [alice_0, bob_0] [] do
          |  contract_0_0 <- createCmd Module.Template
          |  contract_0_2 <- exerciseCmd contract_0_1 (Module.Choice ())
          |  pure (contract_0_0, contract_0_2)""".stripMargin.replace(
          "\r\n",
          "\n",
        )
    }
  }
}
