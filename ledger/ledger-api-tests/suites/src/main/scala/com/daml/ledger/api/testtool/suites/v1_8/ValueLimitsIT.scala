// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.model.Test.{DummyWithAnnotation, WithList, WithMap}
import com.daml.timer.Delayed

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Random

final class ValueLimitsIT extends LedgerTestSuite {

  test(
    "VLLargeSubmittersNumberCreateContract",
    "Create a contract with a large submitters number",
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
        .submitAndWaitRequest(
          actAs = parties.toList,
          readAs = parties.toList,
          commands = DummyWithAnnotation(parties.head, "First submission").create.command,
        )
      _ <- ledger.submitAndWait(request)
      contracts <- ledger.activeContracts(parties.head)
    } yield {
      assertSingleton("Single create contract expected", contracts)
      ()
    }
  })

  test(
    "VLLargeMapInContract",
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
    "VLLargeMapInChoice",
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
    "VLLargeListInContract",
    "Create a contract with a field containing large list",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val elements = (1 to 10000).map(e => f"element_$e%08d")
    for {
      //contract <- ledger.create(alice, WithList(alice, elements))
      _ <- Future.traverse(1 to 10000)(d => Delayed.by((d*10).millis)(()).flatMap(_ => ledger.create(alice, WithList(alice, elements))))
      //_ <- Future.traverse(1 to 10000)(d => Delayed.by(d.millis)(()).flatMap(_ => ledger.exercise(alice, contract.exerciseWithList_Noop)))
      //_ <- ledger.exercise(alice, contract.exerciseWithList_Noop)
    } yield {
      ()
    }
  })

  test(
    "VLLargeListInChoice",
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
