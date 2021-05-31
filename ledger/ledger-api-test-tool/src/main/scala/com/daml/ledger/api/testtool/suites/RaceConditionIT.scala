// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.RaceConditionTests._
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.semantic.RaceTests._
import com.daml.timer.Delayed

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

final class RaceConditionIT extends LedgerTestSuite {
  raceConditionTest(
    "WWDoubleNonTransientCreate",
    "Cannot concurrently create multiple non-transient contracts with the same key",
    runConcurrently = true,
  ) { implicit ec => ledger => alice =>
    val attempts = (1 to 5).toVector
    Future
      .traverse(attempts) { _ =>
        ledger.create(alice, ContractWithKey(alice)).transform(Success(_))
      }
      .map { results =>
        assertSingleton(
          "Successful contract creations",
          results.filter(_.isSuccess),
        )
        ()
      }
  }

  raceConditionTest(
    "WWDoubleArchive",
    "Cannot archive the same contract multiple times",
    runConcurrently = true,
  ) { implicit ec => ledger => alice =>
    val attempts = (1 to 5).toVector
    for {
      contract <- ledger.create(alice, ContractWithKey(alice))
      _ <- Future.traverse(attempts) { _ =>
        ledger.exercise(alice, contract.exerciseContractWithKey_Archive).transform(Success(_))
      }
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._
      assertSingleton(
        "Successful contract archivals",
        transactions.filter(isArchival),
      )
      ()
    }
  }

  raceConditionTest(
    "WWArchiveVsNonTransientCreate",
    "Cannot create a contract with a key if that key is still used by another contract",
    runConcurrently = true,
  ) { implicit ec => ledger => alice =>
    /*
    This test case is intended to catch a race condition ending up in two consecutive successful contract
    create or archive commands. E.g.:
    [create]  <wait>  [archive]-race-[create]
    In case of a bug causing the second [create] to see a partial result of [archive] command we could end up
    with two consecutive successful contract creations.
     */
    for {
      contract <- ledger.create(alice, ContractWithKey(alice))
      _ <- Delayed.by(500.millis)(())
      createFuture = ledger.create(alice, ContractWithKey(alice)).transform(Success(_))
      exerciseFuture = ledger
        .exercise(alice, contract.exerciseContractWithKey_Archive)
        .transform(Success(_))
      _ <- createFuture
      _ <- exerciseFuture
      _ <- ledger.create(
        alice,
        DummyContract(alice),
      ) // Create a dummy contract to ensure that we're not stuck with previous commands
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._

      assert(
        isCreateNonTransient(transactions.head),
        "The first transaction is expected to be a contract creation",
      )
      assert(
        transactions.exists(isCreateDummyContract),
        "A dummy contract creation is missing. A possible reason might be reading the transactions stream in the test case before submitted commands had chance to be processed.",
      )

      val (_, valid) =
        transactions.filterNot(isCreateDummyContract).tail.foldLeft((transactions.head, true)) {
          case ((previousTx, isValidSoFar), currentTx) =>
            if (isValidSoFar) {
              val valid = (isArchival(previousTx) && isCreateNonTransient(
                currentTx
              )) || (isCreateNonTransient(previousTx) && isArchival(currentTx))
              (currentTx, valid)
            } else {
              (previousTx, isValidSoFar)
            }
        }

      if (!valid)
        fail(
          s"""Invalid transaction sequence: ${transactions.map(printTransaction).mkString("\n")}"""
        )
    }
  }

  raceConditionTest(
    "RWTransientCreateVsNonTransientCreate",
    "Cannot create a transient contract and a non-transient contract with the same key",
  ) { implicit ec => ledger => alice =>
    for {
      wrapper <- ledger.create(alice, CreateWrapper(alice))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 20,
        once = ledger.create(alice, ContractWithKey(alice)).map(_ => ()),
        repeated =
          ledger.exercise(alice, wrapper.exerciseCreateWrapper_CreateTransient).map(_ => ()),
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
    "RWArchiveVsNonConsumingChoice",
    "Cannot exercise a choice after a contract archival",
  ) { implicit ec => ledger => alice =>
    for {
      contract <- ledger.create(alice, ContractWithKey(alice))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 10,
        once = ledger.exercise(alice, contract.exerciseContractWithKey_Archive),
        repeated = ledger.exercise(alice, contract.exerciseContractWithKey_Exercise),
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
    "RWArchiveVsFetch",
    "Cannot fetch an archived contract",
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
    "RWArchiveVsLookupByKey",
    "Cannot successfully lookup by key an archived contract",
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
    "RWArchiveVsFailedLookupByKey",
    "Lookup by key cannot fail after a contract creation",
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
