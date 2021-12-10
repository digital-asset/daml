// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.RaceConditionTests._
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value.RecordField
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
      import ExceptionRaceConditionIT.TransactionUtil._

      // We deliberately allow situations where no non-transient contract is created and verify the transactions
      // order when such contract is actually created.
      transactions.find(isCreateNonTransient).foreach { nonTransientCreateTransaction =>
        transactions
          .filter(isRollbackCreate)
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
      import ExceptionRaceConditionIT.TransactionUtil._
      val archivalTransaction = assertSingleton("archivals", transactions.filter(isArchival))
      transactions
        .filter(isRollbackNonConsumingExercise)
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
      import ExceptionRaceConditionIT.TransactionUtil._
      val archivalTransaction = assertSingleton("archivals", transactions.filter(isArchival))
      transactions
        .filter(isRollbackConsumingExercise)
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
      import ExceptionRaceConditionIT.TransactionUtil._
      val archivalTransaction = assertSingleton("archivals", transactions.filter(isArchival))
      transactions
        .filter(isRollbackFetch)
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
      import ExceptionRaceConditionIT.TransactionUtil._
      val archivalTransaction = assertSingleton("archivals", transactions.filter(isArchival))
      transactions
        .filter(isRollbackContractLookup(success = true))
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
      import ExceptionRaceConditionIT.TransactionUtil._
      val createNonTransientTransaction = assertSingleton(
        "create-non-transient transactions",
        transactions.filter(isCreateNonTransient),
      )
      transactions
        .filter(isRollbackContractLookup(success = false))
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

object ExceptionRaceConditionIT {
  object TransactionUtil {

    private implicit class TransactionTreeTestOps(tx: TransactionTree) {
      def hasEventsNumber(expectedNumberOfEvents: Int): Boolean =
        tx.eventsById.size == expectedNumberOfEvents

      def containsEvent(condition: TreeEvent => Boolean): Boolean =
        tx.eventsById.values.toList.exists(condition)
    }

    private def isCreated(templateName: String)(event: TreeEvent): Boolean =
      event.kind.isCreated && event.getCreated.templateId.exists(_.entityName == templateName)

    private def isExerciseEvent(choiceName: String)(event: TreeEvent): Boolean =
      event.kind.isExercised && event.getExercised.choice == choiceName

    // def isCreateDummyContract(tx: TransactionTree): Boolean =
    //   tx.containsEvent(isCreated(RaceTests.DummyContract.TemplateName))

    def isCreateNonTransient(tx: TransactionTree): Boolean =
      tx.hasEventsNumber(1) &&
        tx.containsEvent(isCreated(ExceptionRaceTests.ContractWithKey.TemplateName))

    def isRollbackCreate(tx: TransactionTree): Boolean =
      // Note that we cannot assert that the create is present because
      // rollback nodes are not visible on the ledger API.
      tx.containsEvent(isExerciseEvent(ExceptionRaceTests.CreateWrapper.ChoiceCreateRollback))

    def isArchival(tx: TransactionTree): Boolean =
      tx.hasEventsNumber(1) &&
        tx.containsEvent(isExerciseEvent(ExceptionRaceTests.ContractWithKey.ChoiceArchive))

    def isRollbackNonConsumingExercise(tx: TransactionTree): Boolean =
      tx.hasEventsNumber(1) &&
        tx.containsEvent(
          isExerciseEvent(ExceptionRaceTests.ExerciseWrapper.ChoiceNonConsumingRollback)
        )

    def isRollbackConsumingExercise(tx: TransactionTree): Boolean =
      tx.hasEventsNumber(1) &&
        tx.containsEvent(
          isExerciseEvent(ExceptionRaceTests.ExerciseWrapper.ChoiceConsumingRollback)
        )

    def isRollbackFetch(tx: TransactionTree): Boolean =
      tx.hasEventsNumber(1) &&
        tx.containsEvent(isExerciseEvent(ExceptionRaceTests.FetchWrapper.ChoiceFetch))

    private def isFoundContractField(found: Boolean)(field: RecordField) = {
      field.label == "found" && field.value.exists(_.getBool == found)
    }

    def isRollbackContractLookup(success: Boolean)(tx: TransactionTree): Boolean =
      tx.containsEvent { event =>
        isCreated(ExceptionRaceTests.LookupResult.TemplateName)(event) &&
        event.getCreated.getCreateArguments.fields.exists(isFoundContractField(found = success))
      }
  }

  private object ExceptionRaceTests {
    object ContractWithKey {
      val TemplateName = "ContractWithKey"
      val ChoiceArchive = "ContractWithKey_Archive"
//      val ChoiceExercise = "ContractWithKey_Exercise"
    }

    // object DummyContract {
    //   val TemplateName = "DummyContract"
    // }

    object FetchWrapper {
      val ChoiceFetch = "FetchWrapper_Fetch"
    }

    object LookupResult {
      val TemplateName = "LookupResult"
    }

    object CreateWrapper {
      val ChoiceCreateRollback = "CreateWrapper_CreateRollback"
    }

    object ExerciseWrapper {
      val ChoiceNonConsumingRollback = "ExerciseWrapper_ExerciseNonConsumingRollback"
      val ChoiceConsumingRollback = "ExerciseWrapper_ExerciseConsumingRollback"
    }
  }
}
