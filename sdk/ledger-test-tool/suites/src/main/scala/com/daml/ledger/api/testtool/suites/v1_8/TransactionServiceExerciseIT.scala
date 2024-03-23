// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test.AgreementFactory._
import com.daml.ledger.test.model.Test.CreateAndFetch._
import com.daml.ledger.test.model.Test.Dummy._
import com.daml.ledger.test.model.Test.DummyFactory._
import com.daml.ledger.test.model.Test._
import scalaz.Tag

class TransactionServiceExerciseIT extends LedgerTestSuite {
  test(
    "TXUseCreateToExercise",
    "Should be able to directly use a contract identifier to exercise a choice",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummyFactory <- ledger.create(party, DummyFactory(party))
      transactions <- ledger.exercise(party, dummyFactory.exerciseDummyFactoryCall())
    } yield {
      val events = transactions.rootEventIds.collect(transactions.eventsById)
      val exercised = events.filter(_.kind.isExercised)
      assert(exercised.size == 1, s"Only one exercise expected, got ${exercised.size}")
      assert(
        exercised.head.getExercised.contractId == Tag.unwrap(dummyFactory),
        s"The identifier of the exercised contract should have been ${Tag
            .unwrap(dummyFactory)} but instead it was ${exercised.head.getExercised.contractId}",
      )
    }
  })

  test(
    "TXContractIdFromExerciseWhenFilter",
    "Expose contract identifiers that are results of exercising choices when filtering by template",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      factory <- ledger.create(party, DummyFactory(party))
      _ <- ledger.exercise(party, factory.exerciseDummyFactoryCall())
      dummyWithParam <- ledger.flatTransactionsByTemplateId(DummyWithParam.id, party)
      dummyFactory <- ledger.flatTransactionsByTemplateId(DummyFactory.id, party)
    } yield {
      val create = assertSingleton("GetCreate", dummyWithParam.flatMap(createdEvents))
      assertEquals(
        "Create should be of DummyWithParam",
        create.getTemplateId,
        Tag.unwrap(DummyWithParam.id),
      )
      val archive = assertSingleton("GetArchive", dummyFactory.flatMap(archivedEvents))
      assertEquals(
        "Archive should be of DummyFactory",
        archive.getTemplateId,
        Tag.unwrap(DummyFactory.id),
      )
      assertEquals(
        "Mismatching archived contract identifier",
        archive.contractId,
        Tag.unwrap(factory),
      )
    }
  })

  test(
    "TXNotArchiveNonConsuming",
    "Expressing a non-consuming choice on a contract should not result in its archival",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, receiver), Participant(beta, giver)) =>
    for {
      agreementFactory <- beta.create(giver, AgreementFactory(receiver, giver))
      _ <- eventually("exerciseCreateAgreement") {
        alpha.exercise(receiver, agreementFactory.exerciseCreateAgreement())
      }
      _ <- synchronize(alpha, beta)
      transactions <- alpha.flatTransactions(receiver, giver)
    } yield {
      assert(
        !transactions.exists(_.events.exists(_.event.isArchived)),
        s"The transaction include an archival: ${transactions.flatMap(_.events).filter(_.event.isArchived)}",
      )
    }
  })

  test(
    "TXFetchContractCreatedInTransaction",
    "It should be possible to fetch a contract created within a transaction",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      createAndFetch <- ledger.create(party, CreateAndFetch(party))
      transaction <- ledger.exerciseForFlatTransaction(
        party,
        createAndFetch.exerciseCreateAndFetch_Run(),
      )
    } yield {
      val _ = assertSingleton("There should be only one create", createdEvents(transaction))
      val exercise =
        assertSingleton("There should be only one archive", archivedEvents(transaction))
      assertEquals(
        "The contract identifier of the exercise does not match",
        Tag.unwrap(createAndFetch),
        exercise.contractId,
      )
    }
  })

  test(
    "TXRejectOnFailingAssertion",
    "Reject a transaction on a failing assertion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      failure <- ledger
        .exercise(
          party,
          dummy
            .exerciseConsumeIfTimeIsBetween(Primitive.Timestamp.MAX, Primitive.Timestamp.MAX),
        )
        .mustFail("exercising with a failing assertion")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.CommandExecution.Interpreter.UnhandledException,
        Some("Assertion failed"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })
}
