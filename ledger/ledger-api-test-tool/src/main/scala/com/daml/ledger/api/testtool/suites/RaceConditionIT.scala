package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v1.transaction.TransactionTree
import com.daml.ledger.api.v1.value.RecordField
import com.daml.ledger.test.semantic.RaceTests._
import com.daml.lf.data.{Bytes, Ref}

import scala.concurrent.Future
import scala.util.Success

final class RaceConditionIT extends LedgerTestSuite {

  test(
    "WWDoubleNonTransientCreate",
    "Cannot concurrently create multiple non-transient contracts with the same key",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, alice)) =>
      val Attempts = 100
      val ExpectedNumberOfSuccessfulCreations = 1
      println(s"$ledger $alice $Attempts")
      Future.traverse(1 to Attempts) {
        case _ =>
          ledger.create(alice, ContractWithKey(alice)).transform(Success(_))
      }.map { results =>
        assertLength("Successful contract creations", ExpectedNumberOfSuccessfulCreations, results.filter(_.isSuccess))
        ()
      }
  })

  test(
    "WWDoubleArchive",
    "Cannot concurrently create multiple non-transient contracts with the same key",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, alice)) =>
      val Attempts = 100
      val ExpectedNumberOfSuccessfulArchivals = 1
      val ArchivalChoiceName = "ContractWithKey_Archive"
      for {
        contract <- ledger.create(alice, ContractWithKey(alice))
        _ <- Future.traverse(1 to Attempts) { _ =>
          ledger.exercise(alice, contract.exerciseContractWithKey_Archive).transform(Success(_))
        }
        transactions <- ledger.transactionTrees(alice)
      } yield  {
        def isArchival(transactionTree: TransactionTree): Boolean = {
          transactionTree.eventsById.values.toList match {
            case List(event) if event.kind.isExercised =>
              event.getExercised.choice == ArchivalChoiceName
            case _ => false
          }
        }

        assertLength("Successful contract archivals", ExpectedNumberOfSuccessfulArchivals, transactions.filter(isArchival))
        ()
      }
  })

  test(
    "RWTransientCreateVsNonTransientCreate",
    "Cannot create a transient contract and a non-transient contract with the same key",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, alice)) =>
      // A high number of attempts is set intentionally to increase the total duration of the test to make it possibility
      // of subsequent creations and archivals of a transient contract
      val Attempts = 100
      // The non-transient contract creation is fired almost at the end of all attempts to allow several transient creations
      // to be processed before the non-transient creation
      val ActionAt = 90
      val CreatedContractTemplateName = "ContractWithKey"
      for {
        wrapper <- ledger.create(alice, TransientWrapper(alice))
        _ <- Future.traverse(1 to Attempts) { attempt =>
          if (attempt == ActionAt) {
            ledger.create(alice, ContractWithKey(alice)).map(_ => ()).transform(Success(_))
          } else {
            ledger.exercise(alice, wrapper.exerciseCreateTransientContract).map(_ => ()).transform(Success(_))
          }
        }
        transactions <- ledger.transactionTrees(alice)
      } yield {
        // TODO: add conditions on event template names
        def isTransientCreate(transactionTree: TransactionTree): Boolean =
          transactionTree.eventsById.size == 3

        def isNonTransientCreate(transactionTree: TransactionTree): Boolean = {
          transactionTree.eventsById.values.toList match {
            case List(event) if event.kind.isCreated =>
              event.getCreated.templateId.exists(_.entityName == CreatedContractTemplateName)
            case _ => false
          }
        }

        val nonTransientCreateTransaction = transactions
          .find(isNonTransientCreate)
          .getOrElse(fail("No non-transient create transaction found"))

        transactions
          .filter(isTransientCreate)
          .foreach(assertTransactionOrder(_, nonTransientCreateTransaction))
      }
  })

  test(
    "RWArchiveVsNonConsumingChoice",
    "Cannot exercise a non-consuming choice after a contract archival",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, alice)) =>
      val ArchiveAt = 5
      val Attempts = 10
      val ArchivalChoiceName = "ContractWithChoices_Archive"
      val ExerciseChoiceName = "ContractWithChoices_Exercise"
      for {
        contract <- ledger.create(alice, ContractWithChoices(alice))
        _ <- Future.traverse(1 to Attempts) { attempt =>
          if (attempt == ArchiveAt) {
            ledger.exercise(alice, contract.exerciseContractWithChoices_Archive).transform(Success(_))
          } else {
            ledger.exercise(alice, contract.exerciseContractWithChoices_Exercise).transform(Success(_))
          }
        }
        transactions <- ledger.transactionTrees(alice)
      } yield {
        // TODO: deduplicate these two
        def isArchival(transactionTree: TransactionTree): Boolean = {
          transactionTree.eventsById.values.toList match {
            case List(event) if event.kind.isExercised =>
              event.getExercised.choice == ArchivalChoiceName
            case _ => false
          }
        }
        def isNonConsumingExercise(transactionTree: TransactionTree): Boolean = {
          transactionTree.eventsById.values.toList match {
            case List(event) if event.kind.isExercised =>
              event.getExercised.choice == ExerciseChoiceName
            case _ => false
          }
        }

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
      )(implicit ec => {
        case Participants(Participant(ledger, alice)) =>
          val ArchiveAt = 400
          val Attempts = 500
          // TODO: create a helper module for this
          val ArchivalChoiceName = "ContractWithChoices_Archive"
          val FetchChoiceName = "FetchContractWithChoices_Fetch"
          for {
            contract <- ledger.create(alice, ContractWithChoices(alice))
            fetchConract <- ledger.create(alice, FetchContractWithChoices(alice, contract))
            _ <- Future.traverse(1 to Attempts) { attempt =>
              if (attempt == ArchiveAt) {
                ledger.exercise(alice, contract.exerciseContractWithChoices_Archive).transform(Success(_))
              } else {
                ledger.exercise(alice, fetchConract.exerciseFetchContractWithChoices_Fetch).transform(Success(_))
              }
            }
            transactions <- ledger.transactionTrees(alice)
          } yield {
            // TODO: deduplicate these two
            def isArchival(transactionTree: TransactionTree): Boolean = {
              transactionTree.eventsById.values.toList match {
                case List(event) if event.kind.isExercised =>
                  event.getExercised.choice == ArchivalChoiceName
                case _ => false
              }
            }
            def isFetch(transactionTree: TransactionTree): Boolean = {
              transactionTree.eventsById.values.toList match {
                case List(event) if event.kind.isExercised =>
                  event.getExercised.choice == FetchChoiceName
                case _ => false
              }
            }

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
  )(implicit ec => {
    case Participants(Participant(ledger, alice)) =>
      val ArchiveAt = 90
      val Attempts = 100
      val ArchivalChoiceName = "ContractWithKey_Archive"
      val LookupResultTemplateName = "LookupResult"
      for {
        contract <- ledger.create(alice, ContractWithKey(alice))
        looker <- ledger.create(alice, LookerUpByKey(alice))
        _ <- Future.traverse(1 to Attempts) { attempt =>
          if (attempt == ArchiveAt) {
            ledger.exercise(alice, contract.exerciseContractWithKey_Archive).transform(Success(_))
          } else {
            ledger.exercise(alice, looker.exerciseLookup).transform(Success(_))
          }
        }
        transactions <- ledger.transactionTrees(alice)
      } yield {
        // TODO: deduplicate these two
        def isArchival(transactionTree: TransactionTree): Boolean = {
          transactionTree.eventsById.values.toList match {
            case List(event) if event.kind.isExercised =>
              event.getExercised.choice == ArchivalChoiceName
            case _ => false
          }
        }
        def isSuccessfulContractLookup(transactionTree: TransactionTree): Boolean = {
          def isFoundContractField(field: RecordField) = {
            field.label == "found" && field.value.exists(_.getBool == true)
          }

          transactionTree.eventsById.values.toList.exists { event =>
            event.kind.isCreated && event.getCreated.templateId.get.entityName == LookupResultTemplateName &&
              event.getCreated.getCreateArguments.fields.exists(isFoundContractField)
          }
        }

        val archivalTransaction = transactions.find(isArchival).getOrElse(fail("No archival transaction found"))

        transactions
          .filter(isSuccessfulContractLookup)
          .foreach(assertTransactionOrder(_, archivalTransaction))
      }
  })

  test(
    "RWArchiveVsFailedLookupByKey",
    "Lookup by key cannot fail after a contract creation",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, alice)) =>
      val CreateAt = 90
      val Attempts = 100
      val CreateNonTransientTemplateName = "ContractWithKey"
      val LookupResultTemplateName = "LookupResult"
      for {
        looker <- ledger.create(alice, LookerUpByKey(alice))
        _ <- Future.traverse(1 to Attempts) { attempt =>
          if (attempt == CreateAt) {
            ledger.create(alice, ContractWithKey(alice)).transform(Success(_))
          } else {
            ledger.exercise(alice, looker.exerciseLookup).transform(Success(_))
          }
        }
        transactions <- ledger.transactionTrees(alice)
      } yield {
        //TODO: factor out containEvent(): TreeEvent => Boolean
        def isCreateNonTransient(transactionTree: TransactionTree): Boolean = {
          transactionTree.eventsById.values.toList.exists { event =>
            event.kind.isCreated && event.getCreated.templateId.get.entityName == CreateNonTransientTemplateName
          }
        }
        def isFailedContractLookup(transactionTree: TransactionTree): Boolean = {
          def isNotFoundContractField(field: RecordField) = {
            field.label == "found" && field.value.exists(_.getBool == false)
          }

          transactionTree.eventsById.values.toList.exists { event =>
            event.kind.isCreated && event.getCreated.templateId.get.entityName == LookupResultTemplateName &&
              event.getCreated.getCreateArguments.fields.exists(isNotFoundContractField)
          }
        }

        val createNonTransientTransaction = transactions.find(isCreateNonTransient).getOrElse(fail("No create-non-transient transaction found"))

        transactions
          .filter(isFailedContractLookup)
          .foreach(assertTransactionOrder(_, createNonTransientTransaction))
      }
  })

  def assertTransactionOrder(expectedFirst: TransactionTree, expectedSecond: TransactionTree) = {
    if (offsetLessThan(expectedFirst.offset, expectedSecond.offset)) ()
    else fail(
      s"""Offset ${expectedFirst.offset} is not before ${expectedSecond.offset}
         |
         |Expected first: ${printTransaction(expectedFirst)}
         |Expected second: ${printTransaction(expectedSecond)}
         |""".stripMargin)
  }

  def offsetLessThan(a: String, b: String): Boolean =
    Bytes.ordering.lt(offsetBytes(a), offsetBytes(b))

  def printTransaction(transactionTree: TransactionTree) = {
    s"""Offset: ${transactionTree.offset}, number of events: ${transactionTree.eventsById.size}
       |${transactionTree.eventsById.values.map(e => s" -> $e").mkString("\n")}
       |""".stripMargin
  }

  def offsetBytes(offset: String) = {
    Bytes.fromHexString(Ref.HexString.assertFromString(offset))
  }

}
