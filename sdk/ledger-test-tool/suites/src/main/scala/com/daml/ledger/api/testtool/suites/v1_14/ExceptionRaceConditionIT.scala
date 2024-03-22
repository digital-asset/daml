// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_14

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.RaceConditionTests._
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value.RecordField
import com.daml.ledger.javaapi.data.Party
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.semantic.exceptionracetests._

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

final class ExceptionRaceConditionIT extends LedgerTestSuite {

  import ExceptionRaceConditionIT.ExceptionRaceTests
  import ExceptionRaceConditionIT.CompanionImplicits._

  raceConditionTest(
    "RWRollbackCreateVsNonTransientCreate",
    "Cannot create a contract in a rollback and a non-transient contract with the same key",
  ) { implicit ec => ledger => alice =>
    for {
      wrapper <- ledger.create(alice, new CreateWrapper(alice))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 20,
        once = ledger.create(alice, new ContractWithKey(alice)).map(_ => ()),
        repeated =
          ledger.exercise(alice, wrapper.exerciseCreateWrapper_CreateRollback()).map(_ => ()),
      )
      transactions <- transactions(ledger, alice)
    } yield {
      import ExceptionRaceConditionIT.TransactionUtil._

      // We deliberately allow situations where no non-transient contract is created and verify the transactions
      // order when such contract is actually created.
      transactions.find(isCreate(_, ExceptionRaceTests.ContractWithKey.TemplateName)).foreach {
        nonTransientCreateTransaction =>
          transactions
            .filter(isExercise(_, ExceptionRaceTests.CreateWrapper.ChoiceCreateRollback))
            .foreach(assertTransactionOrder(_, nonTransientCreateTransaction))
      }
    }
  }

  raceConditionTest(
    "RWArchiveVsRollbackNonConsumingChoice",
    "Cannot exercise a non-consuming choice in a rollback after a contract archival",
  ) { implicit ec => ledger => alice =>
    for {
      wrapper: ExerciseWrapper.ContractId <- ledger.create(alice, new ExerciseWrapper(alice))
      contract: ContractWithKey.ContractId <- ledger.create(alice, new ContractWithKey(alice))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 10,
        once = ledger.exercise(alice, contract.exerciseContractWithKey_Archive()),
        repeated = ledger.exercise(
          alice,
          wrapper.exerciseExerciseWrapper_ExerciseNonConsumingRollback(contract),
        ),
      )
      transactions <- transactions(ledger, alice)
    } yield {
      import ExceptionRaceConditionIT.TransactionUtil._
      val archivalTransaction = assertSingleton("archivals", transactions.filter(isArchival))
      transactions
        .filter(isExercise(_, ExceptionRaceTests.ExerciseWrapper.ChoiceNonConsumingRollback))
        .foreach(assertTransactionOrder(_, archivalTransaction))
    }
  }

  raceConditionTest(
    "RWArchiveVsRollbackConsumingChoice",
    "Cannot exercise a consuming choice in a rollback after a contract archival",
  ) { implicit ec => ledger => alice =>
    for {
      wrapper: ExerciseWrapper.ContractId <- ledger.create(alice, new ExerciseWrapper(alice))
      contract: ContractWithKey.ContractId <- ledger.create(alice, new ContractWithKey(alice))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 10,
        once = ledger.exercise(alice, contract.exerciseContractWithKey_Archive()),
        repeated = ledger.exercise(
          alice,
          wrapper.exerciseExerciseWrapper_ExerciseConsumingRollback(contract),
        ),
      )
      transactions <- transactions(ledger, alice)
    } yield {
      import ExceptionRaceConditionIT.TransactionUtil._
      val archivalTransaction = assertSingleton("archivals", transactions.filter(isArchival))
      transactions
        .filter(isExercise(_, ExceptionRaceTests.ExerciseWrapper.ChoiceConsumingRollback))
        .foreach(assertTransactionOrder(_, archivalTransaction))
    }
  }

  raceConditionTest(
    "RWArchiveVsRollbackFetch",
    "Cannot fetch in a rollback after a contract archival",
  ) { implicit ec => ledger => alice =>
    for {
      contract: ContractWithKey.ContractId <- ledger.create(alice, new ContractWithKey(alice))
      fetchConract: FetchWrapper.ContractId <- ledger.create(
        alice,
        new FetchWrapper(alice, contract),
      )
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 10,
        once = ledger.exercise(alice, contract.exerciseContractWithKey_Archive()),
        repeated = ledger.exercise(alice, fetchConract.exerciseFetchWrapper_Fetch()),
      )
      transactions <- transactions(ledger, alice)
    } yield {
      import ExceptionRaceConditionIT.TransactionUtil._
      val archivalTransaction = assertSingleton("archivals", transactions.filter(isArchival))
      transactions
        .filter(isExercise(_, ExceptionRaceTests.FetchWrapper.ChoiceFetch))
        .foreach(assertTransactionOrder(_, archivalTransaction))
    }
  }

  raceConditionTest(
    "RWArchiveVsRollbackLookupByKey",
    "Cannot successfully lookup by key in a rollback after a contract archival",
  ) { implicit ec => ledger => alice =>
    for {
      contract: ContractWithKey.ContractId <- ledger.create(alice, new ContractWithKey(alice))
      looker: LookupWrapper.ContractId <- ledger.create(alice, new LookupWrapper(alice))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 20,
        once = ledger.exercise(alice, contract.exerciseContractWithKey_Archive()),
        repeated = ledger.exercise(alice, looker.exerciseLookupWrapper_Lookup()),
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
      looker: LookupWrapper.ContractId <- ledger.create(alice, new LookupWrapper(alice))
      _ <- executeRepeatedlyWithRandomDelay(
        numberOfAttempts = 5,
        once = ledger.create(alice, new ContractWithKey(alice)),
        repeated = ledger.exercise(alice, looker.exerciseLookupWrapper_Lookup()),
      ): @nowarn("cat=lint-infer-any")
      transactions <- transactions(ledger, alice)
    } yield {
      import ExceptionRaceConditionIT.TransactionUtil._
      val createNonTransientTransaction = assertSingleton(
        "create-non-transient transactions",
        transactions.filter(isCreate(_, ExceptionRaceTests.ContractWithKey.TemplateName)),
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
  )(testCase: ExecutionContext => ParticipantTestContext => Party => Future[Unit]): Unit =
    test(
      shortIdentifier = shortIdentifier,
      description = description,
      partyAllocation = allocate(SingleParty),
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

    def isCreate(tx: TransactionTree, templateName: String): Boolean =
      tx.hasEventsNumber(1) &&
        tx.containsEvent(isCreated(templateName))

    def isExercise(tx: TransactionTree, choiceName: String): Boolean =
      tx.hasEventsNumber(1) &&
        tx.containsEvent(isExerciseEvent(choiceName))

    def isArchival(tx: TransactionTree): Boolean =
      tx.hasEventsNumber(1) &&
        tx.containsEvent(isExerciseEvent(ExceptionRaceTests.ContractWithKey.ChoiceArchive))

    private def isFoundContractField(found: Boolean)(field: RecordField) = {
      field.label == "found" && field.value.exists(_.getBool == found)
    }

    def isRollbackContractLookup(success: Boolean)(tx: TransactionTree): Boolean =
      tx.containsEvent { event =>
        isCreated(ExceptionRaceTests.LookupResult.TemplateName)(event) &&
        event.getCreated.getCreateArguments.fields.exists(isFoundContractField(found = success))
      }
  }

  object ExceptionRaceTests {
    object ContractWithKey {
      val TemplateName = "ContractWithKey"
      val ChoiceArchive = "ContractWithKey_Archive"
    }

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

  private object CompanionImplicits {
    implicit val createWrapperCompanion: ContractCompanion.WithoutKey[
      CreateWrapper.Contract,
      CreateWrapper.ContractId,
      CreateWrapper,
    ] = CreateWrapper.COMPANION
    implicit val contractWithKeyCompanion: ContractCompanion.WithKey[
      ContractWithKey.Contract,
      ContractWithKey.ContractId,
      ContractWithKey,
      String,
    ] = ContractWithKey.COMPANION
    implicit val lookupWrapperCompanion: ContractCompanion.WithoutKey[
      LookupWrapper.Contract,
      LookupWrapper.ContractId,
      LookupWrapper,
    ] = LookupWrapper.COMPANION
    implicit val fetchWrapperCompanion: ContractCompanion.WithoutKey[
      FetchWrapper.Contract,
      FetchWrapper.ContractId,
      FetchWrapper,
    ] = FetchWrapper.COMPANION
    implicit val exerciseWrapperCompanion: ContractCompanion.WithoutKey[
      ExerciseWrapper.Contract,
      ExerciseWrapper.ContractId,
      ExerciseWrapper,
    ] = ExerciseWrapper.COMPANION
  }

}
