// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value.RecordField
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.semantic.RaceTests._
import com.daml.lf.data.{Bytes, Ref}
import com.daml.timer.Delayed

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Success

final class RaceConditionIT extends LedgerTestSuite {

  private val DefaultRepetitionsNumber: Int = 5
  private val WaitBeforeGettingTransactions = 500.millis

  test(
    "WWDoubleNonTransientCreate",
    "Cannot concurrently create multiple non-transient contracts with the same key",
    allocate(SingleParty),
    repeated = DefaultRepetitionsNumber,
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val Attempts = 5
    val ExpectedNumberOfSuccessfulCreations = 1
    Future
      .traverse(1 to Attempts) { _ =>
        ledger.create(alice, ContractWithKey(alice)).transform(Success(_))
      }
      .map { results =>
        assertLength(
          "Successful contract creations",
          ExpectedNumberOfSuccessfulCreations,
          results.filter(_.isSuccess),
        )
        ()
      }
  })

  test(
    "WWDoubleArchive",
    "Cannot archive the same contract multiple times",
    allocate(SingleParty),
    repeated = DefaultRepetitionsNumber,
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val Attempts = 5
    val ExpectedNumberOfSuccessfulArchivals = 1
    for {
      contract <- ledger.create(alice, ContractWithKey(alice))
      _ <- Future.traverse(1 to Attempts) { _ =>
        ledger.exercise(alice, contract.exerciseContractWithKey_Archive).transform(Success(_))
      }
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._
      assertLength(
        "Successful contract archivals",
        ExpectedNumberOfSuccessfulArchivals,
        transactions.filter(isArchival),
      )
      ()
    }
  })

  test(
    "WWArchiveVsNonTransientCreate",
    "Cannot create a contract with a key if that key is still used by another contract",
    allocate(SingleParty),
    repeated = DefaultRepetitionsNumber,
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    /*
    This test case is intended to catch a race condition ending up in two consecutive successful contract
    create or archive commands. E.g.:
    [create]  <wait>  [archive]-race-[create]
    In case of a bug causing the second [create] to see a partial result of [archive] command we could end up
    with two consecutive successful contract creations.
     */
    for {
      contract <- ledger.create(alice, ContractWithKey(alice))
      _ <- Delayed.by(1.second)(())
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
  })

  test(
    "RWTransientCreateVsNonTransientCreate",
    "Cannot create a transient contract and a non-transient contract with the same key",
    allocate(SingleParty),
    repeated = DefaultRepetitionsNumber,
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val Attempts = 100
    val ActionAt = 90
    for {
      wrapper <- ledger.create(alice, CreateWrapper(alice))
      _ <- Future.traverse(1 to Attempts) { attempt =>
        if (attempt == ActionAt) {
          ledger.create(alice, ContractWithKey(alice)).map(_ => ()).transform(Success(_))
        } else {
          ledger
            .exercise(alice, wrapper.exerciseCreateWrapper_CreateTransient)
            .map(_ => ())
            .transform(Success(_))
        }
      }
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._

      val nonTransientCreateTransaction = transactions
        .find(isCreateNonTransient)
        .getOrElse(fail("No non-transient create transaction found"))

      transactions
        .filter(isTransientCreate)
        .foreach(assertTransactionOrder(_, nonTransientCreateTransaction))
    }
  })

  test(
    "RWArchiveVsNonConsumingChoice",
    "Cannot exercise a choice after a contract archival",
    allocate(SingleParty),
    repeated = DefaultRepetitionsNumber,
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val ArchiveAt = 5
    val Attempts = 10
    for {
      contract <- ledger.create(alice, ContractWithKey(alice))
      _ <- Future.traverse(1 to Attempts) { attempt =>
        if (attempt == ArchiveAt) {
          ledger.exercise(alice, contract.exerciseContractWithKey_Archive).transform(Success(_))
        } else {
          ledger.exercise(alice, contract.exerciseContractWithKey_Exercise).transform(Success(_))
        }
      }
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._

      val archivalTransaction = transactions
        .find(isArchival)
        .getOrElse(fail("No archival transaction found"))

      transactions
        .filter(isNonConsumingExercise)
        .foreach(assertTransactionOrder(_, archivalTransaction))

    }
  })

  test(
    "RWArchiveVsFetch",
    "Cannot fetch an archived contract",
    allocate(SingleParty),
    repeated = DefaultRepetitionsNumber,
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val ArchiveAt = 400
    val Attempts = 500
    for {
      contract <- ledger.create(alice, ContractWithKey(alice))
      fetchConract <- ledger.create(alice, FetchWrapper(alice, contract))
      _ <- Future.traverse(1 to Attempts) { attempt =>
        if (attempt == ArchiveAt) {
          ledger.exercise(alice, contract.exerciseContractWithKey_Archive).transform(Success(_))
        } else {
          ledger.exercise(alice, fetchConract.exerciseFetchWrapper_Fetch).transform(Success(_))
        }
      }
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._

      val archivalTransaction = transactions
        .find(isArchival)
        .getOrElse(fail("No archival transaction found"))

      transactions
        .filter(isFetch)
        .foreach(assertTransactionOrder(_, archivalTransaction))
    }
  })

  test(
    "RWArchiveVsLookupByKey",
    "Cannot successfully lookup by key an archived contract",
    allocate(SingleParty),
    repeated = DefaultRepetitionsNumber,
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val ArchiveAt = 90
    val Attempts = 100
    for {
      contract <- ledger.create(alice, ContractWithKey(alice))
      looker <- ledger.create(alice, LookupWrapper(alice))
      _ <- Future.traverse(1 to Attempts) { attempt =>
        if (attempt == ArchiveAt) {
          ledger.exercise(alice, contract.exerciseContractWithKey_Archive).transform(Success(_))
        } else {
          ledger.exercise(alice, looker.exerciseLookupWrapper_Lookup).transform(Success(_))
        }
      }
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._

      val archivalTransaction =
        transactions.find(isArchival).getOrElse(fail("No archival transaction found"))

      transactions
        .filter(isSuccessfulContractLookup(success = true))
        .foreach(assertTransactionOrder(_, archivalTransaction))
    }
  })

  test(
    "RWArchiveVsFailedLookupByKey",
    "Lookup by key cannot fail after a contract creation",
    allocate(SingleParty),
    repeated = DefaultRepetitionsNumber,
  )(implicit ec => { case Participants(Participant(ledger, alice)) =>
    val CreateAt = 90
    val Attempts = 100
    for {
      looker <- ledger.create(alice, LookupWrapper(alice))
      _ <- Future.traverse(1 to Attempts) { attempt =>
        if (attempt == CreateAt) {
          ledger.create(alice, ContractWithKey(alice)).transform(Success(_))
        } else {
          ledger.exercise(alice, looker.exerciseLookupWrapper_Lookup).transform(Success(_))
        }
      }
      transactions <- transactions(ledger, alice)
    } yield {
      import TransactionUtil._

      val createNonTransientTransaction = transactions
        .find(isCreateNonTransient)
        .getOrElse(fail("No create-non-transient transaction found"))

      transactions
        .filter(isSuccessfulContractLookup(success = false))
        .foreach(assertTransactionOrder(_, createNonTransientTransaction))
    }
  })

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

    def isCreateDummyContract(tx: TransactionTree): Boolean =
      tx.containsEvent(isCreated(RaceTests.DummyContract.TemplateName))

    def isCreateNonTransient(tx: TransactionTree): Boolean =
      tx.hasEventsNumber(1) &&
        tx.containsEvent(isCreated(RaceTests.ContractWithKey.TemplateName))

    def isTransientCreate(tx: TransactionTree): Boolean =
      tx.containsEvent(isExerciseEvent(RaceTests.CreateWrapper.ChoiceCreateTransient)) &&
        tx.containsEvent(isCreated(RaceTests.ContractWithKey.TemplateName))

    def isArchival(tx: TransactionTree): Boolean =
      tx.hasEventsNumber(1) &&
        tx.containsEvent(isExerciseEvent(RaceTests.ContractWithKey.ChoiceArchive))

    def isNonConsumingExercise(tx: TransactionTree): Boolean =
      tx.hasEventsNumber(1) &&
        tx.containsEvent(isExerciseEvent(RaceTests.ContractWithKey.ChoiceExercise))

    def isFetch(tx: TransactionTree): Boolean =
      tx.hasEventsNumber(1) &&
        tx.containsEvent(isExerciseEvent(RaceTests.FetchWrapper.ChoiceFetch))

    private def isFoundContractField(found: Boolean)(field: RecordField) = {
      field.label == "found" && field.value.exists(_.getBool == found)
    }

    def isSuccessfulContractLookup(success: Boolean)(tx: TransactionTree): Boolean =
      tx.containsEvent { event =>
        isCreated(RaceTests.LookupResult.TemplateName)(event) &&
        event.getCreated.getCreateArguments.fields.exists(isFoundContractField(found = success))
      }

  }

  private def transactions(ledger: ParticipantTestContext, party: Primitive.Party)(implicit
      ec: ExecutionContext
  ) =
    Delayed.by(WaitBeforeGettingTransactions)(()).flatMap(_ => ledger.transactionTrees(party))

  private def assertTransactionOrder(
      expectedFirst: TransactionTree,
      expectedSecond: TransactionTree,
  ): Unit = {
    if (offsetLessThan(expectedFirst.offset, expectedSecond.offset)) ()
    else fail(s"""Offset ${expectedFirst.offset} is not before ${expectedSecond.offset}
         |
         |Expected first: ${printTransaction(expectedFirst)}
         |Expected second: ${printTransaction(expectedSecond)}
         |""".stripMargin)
  }

  private def printTransaction(transactionTree: TransactionTree): String = {
    s"""Offset: ${transactionTree.offset}, number of events: ${transactionTree.eventsById.size}
       |${transactionTree.eventsById.values.map(e => s" -> $e").mkString("\n")}
       |""".stripMargin
  }

  private def offsetLessThan(a: String, b: String): Boolean =
    Bytes.ordering.lt(offsetBytes(a), offsetBytes(b))

  private def offsetBytes(offset: String): Bytes = {
    Bytes.fromHexString(Ref.HexString.assertFromString(offset))
  }

  object RaceTests {
    object ContractWithKey {
      val TemplateName = "ContractWithKey"
      val ChoiceArchive = "ContractWithKey_Archive"
      val ChoiceExercise = "ContractWithKey_Exercise"
    }

    object DummyContract {
      val TemplateName = "DummyContract"
    }

    object FetchWrapper {
      val ChoiceFetch = "FetchWrapper_Fetch"
    }

    object LookupResult {
      val TemplateName = "LookupResult"
    }

    object CreateWrapper {
      val ChoiceCreateTransient = "CreateWrapper_CreateTransient"
    }
  }

}
