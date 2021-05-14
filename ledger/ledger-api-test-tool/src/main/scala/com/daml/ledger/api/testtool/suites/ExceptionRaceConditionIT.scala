// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.RaceConditionTests._
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.semantic.ExceptionRaceTests._

import scala.concurrent.{ExecutionContext, Future}

final class ExceptionRaceConditionIT extends LedgerTestSuite {
  raceConditionTest(
    "RWRollbackCreateVsNonTransientCreate",
    "Cannot create a contract in a rollback and a non-transient contract with the same key",
  ) { implicit ec => ledger => alice =>
    for {
      wrapper <- ledger.create(alice, CreateWrapper(alice))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 20,
        once = ledger.create(alice, ContractWithKey(alice)).map(_ => ()),
        repeated = ledger.exercise(alice, wrapper.exerciseCreateWrapper_CreateRollback).map(_ => ()),
      )
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._

      // We deliberately allow situations where no non-transient contract is created and verify the transactions
      // order when such contract is actually created.
      transactions.find(isCreateNonTransient).foreach { nonTransientCreateTransaction =>
        transactions
          .filter(isTransientCreate)
          .foreach(assertTransactionOrder(_, nonTransientCreateTransaction))
      }
    }
  }

  raceConditionTest(
    "RWArchiveVsRollbackNonConsumingChoice",
    "Cannot exercise a non-consuming choice in a rollback after a contract archival",
  ) { implicit ec => ledger => alice =>
    for {
      wrapper <- ledger.create(alice, ExerciseWrapper(alice))
      contract <- ledger.create(alice, ContractWithKey(alice))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 10,
        once = ledger.exercise(alice, contract.exerciseContractWithKey_Archive),
        repeated = ledger.exercise(
          alice,
          wrapper.exerciseExerciseWrapper_ExerciseNonConsumingRollback(_, contract),
        ),
      )
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._
      val archivalTransaction = assertSingleton("archivals", transactions.filter(isArchival))
      transactions
        .filter(isNonConsumingExercise)
        .foreach(assertTransactionOrder(_, archivalTransaction))
    }
  }

  raceConditionTest(
    "RWArchiveVsRollbackConsumingChoice",
    "Cannot exercise a consuming choice in a rollback after a contract archival",
  ) { implicit ec => ledger => alice =>
    for {
      wrapper <- ledger.create(alice, ExerciseWrapper(alice))
      contract <- ledger.create(alice, ContractWithKey(alice))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 10,
        once = ledger.exercise(alice, contract.exerciseContractWithKey_Archive),
        repeated = ledger.exercise(
          alice,
          wrapper.exerciseExerciseWrapper_ExerciseConsumingRollback(_, contract),
        ),
      )
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._
      val archivalTransaction = assertSingleton("archivals", transactions.filter(isArchival))
      transactions
        .filter(isNonConsumingExercise)
        .foreach(assertTransactionOrder(_, archivalTransaction))
    }
  }

  raceConditionTest(
    "RWArchiveVsRollbackFetch",
    "Cannot fetch in a rollback after a contract archival",
  ) { implicit ec => ledger => alice =>
    for {
      contract <- ledger.create(alice, ContractWithKey(alice))
      fetchConract <- ledger.create(alice, FetchWrapper(alice, contract))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 10,
        once = ledger.exercise(alice, contract.exerciseContractWithKey_Archive),
        repeated = ledger.exercise(alice, fetchConract.exerciseFetchWrapper_Fetch),
      )
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._
      val archivalTransaction = assertSingleton("archivals", transactions.filter(isArchival))
      transactions
        .filter(isFetch)
        .foreach(assertTransactionOrder(_, archivalTransaction))
    }
  }

  raceConditionTest(
    "RWArchiveVsRollbackLookupByKey",
    "Cannot successfully lookup by key in a rollback after a contract archival",
  ) { implicit ec => ledger => alice =>
    for {
      contract <- ledger.create(alice, ContractWithKey(alice))
      looker <- ledger.create(alice, LookupWrapper(alice))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 20,
        once = ledger.exercise(alice, contract.exerciseContractWithKey_Archive),
        repeated = ledger.exercise(alice, looker.exerciseLookupWrapper_Lookup),
      )
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._
      val archivalTransaction = assertSingleton("archivals", transactions.filter(isArchival))
      transactions
        .filter(isContractLookup(success = true))
        .foreach(assertTransactionOrder(_, archivalTransaction))
    }
  }

  raceConditionTest(
    "RWArchiveVsRollbackFailedLookupByKey",
    "Lookup by key in a rollback cannot fail after a contract creation",
  ) { implicit ec => ledger => alice =>
    for {
      looker <- ledger.create(alice, LookupWrapper(alice))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 5,
        once = ledger.create(alice, ContractWithKey(alice)),
        repeated = ledger.exercise(alice, looker.exerciseLookupWrapper_Lookup),
      )
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._
      val createNonTransientTransaction = assertSingleton(
        "create-non-transient transactions",
        transactions.filter(isCreateNonTransient),
      )
      transactions
        .filter(isContractLookup(success = false))
        .foreach(assertTransactionOrder(_, createNonTransientTransaction))
    }
  }

  private def raceConditionTest(
      shortIdentifier: String,
      description: String,
      repeated: Int = DefaultRepetitionsNumber,
      runConcurrently: Boolean = false,
  )(testCase: ExecutionContext => ParticipantTestContext => Primitive.Party => Future[Unit]): Unit =
    test(
      shortIdentifier = shortIdentifier,
      description = description,
      participants = allocate(SingleParty),
      repeated = repeated,
      runConcurrently = runConcurrently,
    )(implicit ec => { case Participants(Participant(ledger, party)) =>
      testCase(ec)(ledger)(party)
    })
}
