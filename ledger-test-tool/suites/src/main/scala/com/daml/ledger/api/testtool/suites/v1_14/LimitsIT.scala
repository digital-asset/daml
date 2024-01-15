// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_14

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.java.semantic.limits.{WithList, WithMap}

import scala.jdk.CollectionConverters._

final class LimitsIT extends LedgerTestSuite {

  test(
    "LLargeMapInContract",
    "Create a contract with a field containing large map",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val elements = (1 to 10000).map(e => (f"element_$e%08d", alice.getValue)).toMap.asJava
    for {
      contract: WithMap.ContractId <- ledger.create(alice, new WithMap(alice, elements))(
        WithMap.COMPANION
      )
      _ <- ledger.exercise(alice, contract.exerciseWithMap_Noop())
    } yield {
      ()
    }
  })

  test(
    "LLargeMapInChoice",
    "Exercise a choice with a large map",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val elements = (1 to 10000).map(e => (f"element_$e%08d", alice.getValue)).toMap.asJava
    for {
      contract: WithMap.ContractId <- ledger.create(
        alice,
        new WithMap(alice, Map.empty[String, String].asJava),
      )(WithMap.COMPANION)
      _ <- ledger.exercise(alice, contract.exerciseWithMap_Expand(elements))
    } yield {
      ()
    }
  })

  test(
    "LLargeListInContract",
    "Create a contract with a field containing large list",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val elements = (1 to 10000).map(e => f"element_$e%08d").asJava
    for {
      contract: WithList.ContractId <- ledger.create(alice, new WithList(alice, elements))(
        WithList.COMPANION
      )
      _ <- ledger.exercise(alice, contract.exerciseWithList_Noop())
    } yield {
      ()
    }
  })

  test(
    "LLargeListInChoice",
    "Exercise a choice with a large list",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val elements = (1 to 10000).map(e => f"element_$e%08d").asJava
    for {
      contract: WithList.ContractId <- ledger
        .create(alice, new WithList(alice, List.empty[String].asJava))(WithList.COMPANION)
      _ <- ledger.exercise(alice, contract.exerciseWithList_Expand(elements))
    } yield {
      ()
    }
  })

}
