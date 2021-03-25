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
    "Cannot archive the same contract multiple times",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, alice)) =>
      val Attempts = 100
      val ExpectedNumberOfSuccessfulArchivals = 1
      for {
        contract <- ledger.create(alice, ContractWithKey(alice))
        _ <- Future.traverse(1 to Attempts) { _ =>
          ledger.exercise(alice, contract.exerciseContractWithKey_Archive).transform(Success(_))
        }
        transactions <- ledger.transactionTrees(alice)
      } yield  {
        import TransactionUtil._
        assertLength("Successful contract archivals", ExpectedNumberOfSuccessfulArchivals, transactions.filter(isArchival))
        ()
      }
  })

  test(
    "WWArchiveVsNonTransientCreate",
    "Cannot re-create a contract if it hasn't yet been archived",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, alice)) =>
      val Attempts = 100
      for {
        contract <- ledger.create(alice, ContractWithKey(alice))
        _ <- Future.traverse(1 to Attempts) { attempt =>
          if (attempt % 2 == 1) {
            ledger.create(alice, ContractWithKey(alice)).transform(Success(_))
          } else {
            ledger.exercise(alice, contract.exerciseContractWithKey_Archive).transform(Success(_))
          }
        }
        transactions <- ledger.transactionTrees(alice)
      } yield  {
        import TransactionUtil._
        def txLt(tx1: TransactionTree, tx2: TransactionTree): Boolean =
          offsetLessThan(tx1.offset, tx2.offset)

        val sorted = transactions.sortWith(txLt)

        assert(isCreateNonTransient(sorted.head), "The first transaction is expected to be a contract creation")
        assert(sorted.tail.nonEmpty, "Several transactions expected")

        val (_, valid) = sorted.tail.foldLeft((sorted.head, true)) {
          case ((previousTx, validUntilNow), currentTx) =>
            if (validUntilNow) {
              val valid = (isArchival(previousTx) && isCreateNonTransient(currentTx)) || (isCreateNonTransient(previousTx) && isArchival(currentTx))
              (currentTx, valid)
            } else {
              (previousTx, validUntilNow)
            }
        }

        if (!valid) fail(s"""Invalid transaction sequence: ${sorted.map(printTransaction).mkString("\n")}""")
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
      for {
        wrapper <- ledger.create(alice, CreateWrapper(alice))
        _ <- Future.traverse(1 to Attempts) { attempt =>
          if (attempt == ActionAt) {
            ledger.create(alice, ContractWithKey(alice)).map(_ => ()).transform(Success(_))
          } else {
            ledger.exercise(alice, wrapper.exerciseCreateWrapper_CreateTransient).map(_ => ()).transform(Success(_))
          }
        }
        transactions <- ledger.transactionTrees(alice)
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
    "Cannot exercise a non-consuming choice after a contract archival",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, alice)) =>
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
        transactions <- ledger.transactionTrees(alice)
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
      )(implicit ec => {
        case Participants(Participant(ledger, alice)) =>
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
            transactions <- ledger.transactionTrees(alice)
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
  )(implicit ec => {
    case Participants(Participant(ledger, alice)) =>
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
        transactions <- ledger.transactionTrees(alice)
      } yield {
        import TransactionUtil._

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
      for {
        looker <- ledger.create(alice, LookupWrapper(alice))
        _ <- Future.traverse(1 to Attempts) { attempt =>
          if (attempt == CreateAt) {
            ledger.create(alice, ContractWithKey(alice)).transform(Success(_))
          } else {
            ledger.exercise(alice, looker.exerciseLookupWrapper_Lookup).transform(Success(_))
          }
        }
        transactions <- ledger.transactionTrees(alice)
      } yield {
        import TransactionUtil._

        val createNonTransientTransaction = transactions.find(isCreateNonTransient).getOrElse(fail("No create-non-transient transaction found"))

        transactions
          .filter(isFailedContractLookup)
          .foreach(assertTransactionOrder(_, createNonTransientTransaction))
      }
  })

  object TransactionUtil {
    //TODO: factor out containEvent(): TreeEvent => Boolean
    def isCreateNonTransient(transactionTree: TransactionTree): Boolean = {
      transactionTree.eventsById.size == 1 &&
        transactionTree.eventsById.values.toList.exists { event =>
        event.kind.isCreated && event.getCreated.templateId.exists(_.entityName == RaceTests.ContractWithKey.TemplateName)
      }
    }

    // TODO: use checking event content
    def isTransientCreate(transactionTree: TransactionTree): Boolean = {
      transactionTree.eventsById.size == 3
    }

    def isArchival(transactionTree: TransactionTree): Boolean = {
      transactionTree.eventsById.values.toList match {
        case List(event) if event.kind.isExercised =>
          event.getExercised.choice == RaceTests.ContractWithKey.ChoiceArchive
        case _ => false
      }
    }

    def isNonConsumingExercise(transactionTree: TransactionTree): Boolean = {
      transactionTree.eventsById.values.toList match {
        case List(event) if event.kind.isExercised =>
          event.getExercised.choice == RaceTests.ContractWithKey.ChoiceExercise
        case _ => false
      }
    }

    def isFetch(transactionTree: TransactionTree): Boolean = {
      transactionTree.eventsById.values.toList match {
        case List(event) if event.kind.isExercised =>
          event.getExercised.choice == RaceTests.FetchWrapper.ChoiceFetch
        case _ => false
      }
    }

    // TODO: deduplicate following two
    def isSuccessfulContractLookup(transactionTree: TransactionTree): Boolean = {
      def isFoundContractField(field: RecordField) = {
        field.label == "found" && field.value.exists(_.getBool == true)
      }

      transactionTree.eventsById.values.toList.exists { event =>
        event.kind.isCreated && event.getCreated.templateId.get.entityName == RaceTests.LookupResult.TemplateName &&
          event.getCreated.getCreateArguments.fields.exists(isFoundContractField)
      }
    }

    def isFailedContractLookup(transactionTree: TransactionTree): Boolean = {
      def isNotFoundContractField(field: RecordField) = {
        field.label == "found" && field.value.exists(_.getBool == false)
      }

      transactionTree.eventsById.values.toList.exists { event =>
        event.kind.isCreated && event.getCreated.templateId.get.entityName == RaceTests.LookupResult.TemplateName &&
          event.getCreated.getCreateArguments.fields.exists(isNotFoundContractField)
      }
    }

  }

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

  object RaceTests {
    object ContractWithKey {
      val TemplateName = "ContractWithKey"
      val ChoiceArchive = "ContractWithKey_Archive"
      val ChoiceExercise = "ContractWithKey_Exercise"
    }

    object FetchWrapper {
      //TODO rename this choice to FetchWrapper_Fetch
      val ChoiceFetch = "FetchContractWithKey_Fetch"
    }

    object LookupResult {
      val TemplateName = "LookupResult"
    }
  }

}
