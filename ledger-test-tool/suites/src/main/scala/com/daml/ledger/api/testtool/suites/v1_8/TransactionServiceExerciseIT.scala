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
import com.daml.ledger.test.java.model.test._

class TransactionServiceExerciseIT extends LedgerTestSuite {
  import CompanionImplicits._

  test(
    "TXUseCreateToExercise",
    "Should be able to directly use a contract identifier to exercise a choice",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummyFactory <- ledger.create(party, new DummyFactory(party))
      transactions <- ledger.exercise(party, dummyFactory.exerciseDummyFactoryCall())
    } yield {
      val events = transactions.rootEventIds.collect(transactions.eventsById)
      val exercised = events.filter(_.kind.isExercised)
      assert(exercised.size == 1, s"Only one exercise expected, got ${exercised.size}")
      assert(
        exercised.head.getExercised.contractId == dummyFactory.contractId,
        s"The identifier of the exercised contract should have been ${dummyFactory.contractId} but instead it was ${exercised.head.getExercised.contractId}",
      )
    }
  })

  test(
    "TXContractIdFromExerciseWhenFilter",
    "Expose contract identifiers that are results of exercising choices when filtering by template",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      factory <- ledger.create(party, new DummyFactory(party))
      _ <- ledger.exercise(party, factory.exerciseDummyFactoryCall())
      dummyWithParam <- ledger.flatTransactionsByTemplateId(DummyWithParam.TEMPLATE_ID, party)
      dummyFactory <- ledger.flatTransactionsByTemplateId(DummyFactory.TEMPLATE_ID, party)
    } yield {
      val create = assertSingleton("GetCreate", dummyWithParam.flatMap(createdEvents))
      assertEquals(
        "Create should be of DummyWithParam",
        create.getTemplateId,
        DummyWithParam.TEMPLATE_ID.toV1,
      )
      val archive = assertSingleton("GetArchive", dummyFactory.flatMap(archivedEvents))
      assertEquals(
        "Archive should be of DummyFactory",
        archive.getTemplateId,
        DummyFactory.TEMPLATE_ID.toV1,
      )
      assertEquals(
        "Mismatching archived contract identifier",
        archive.contractId,
        factory.contractId,
      )
    }
  })

  test(
    "TXNotArchiveNonConsuming",
    "Expressing a non-consuming choice on a contract should not result in its archival",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, receiver), Participant(beta, giver)) =>
    for {
      agreementFactory: AgreementFactory.ContractId <- beta.create(
        giver,
        new AgreementFactory(receiver, giver),
      )
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
      createAndFetch: CreateAndFetch.ContractId <- ledger.create(party, new CreateAndFetch(party))(
        CreateAndFetch.COMPANION
      )
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
        createAndFetch.contractId,
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
      dummy: Dummy.ContractId <- ledger.create(party, new Dummy(party))
      failure <- ledger
        .exercise(
          party,
          dummy
            .exerciseConsumeIfTimeIsBetween(TimestampConversion.MAX, TimestampConversion.MAX),
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
