// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_14

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.semantic.Limits.{WithList, WithMap}

final class LimitsIT extends LedgerTestSuite {

  test(
    "LLargeMapInContract",
    "Create a contract with a field containing large map",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val elements = (1 to 10000).map(e => (f"element_$e%08d", alice)).toMap
    for {
      contract <- ledger.create(alice, WithMap(alice, elements))
      _ <- ledger.exercise(alice, contract.exerciseWithMap_Noop)
    } yield {
      ()
    }
  })

  test(
    "LLargeMapInChoice",
    "Exercise a choice with a large map",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val elements = (1 to 10000).map(e => (f"element_$e%08d", alice)).toMap
    for {
      contract <- ledger.create(alice, WithMap(alice, Map.empty[String, Party]))
      _ <- ledger.exercise(alice, contract.exerciseWithMap_Expand(_, elements))
    } yield {
      ()
    }
  })

  test(
    "LLargeListInContract",
    "Create a contract with a field containing large list",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val elements = (1 to 10000).map(e => f"element_$e%08d")
    for {
      contract <- ledger.create(alice, WithList(alice, elements))
      _ <- ledger.exercise(alice, contract.exerciseWithList_Noop)
    } yield {
      ()
    }
  })

  test(
    "LLargeListInChoice",
    "Exercise a choice with a large list",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val elements = (1 to 10000).map(e => f"element_$e%08d")
    for {
      contract <- ledger.create(alice, WithList(alice, List.empty[String]))
      _ <- ledger.exercise(alice, contract.exerciseWithList_Expand(_, elements))
    } yield {
      ()
    }
  })

}
