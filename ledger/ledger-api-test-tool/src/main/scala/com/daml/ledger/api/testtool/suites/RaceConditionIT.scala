package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v1.transaction.TransactionTree
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

        val nonTransientCreateOffset = transactions
          .find(isNonTransientCreate)
          .map(_.offset)
          .getOrElse(fail("No non-transient create transaction found"))

        transactions
          .filter(isTransientCreate)
          .map(_.offset)
          .foreach(assertOffsetOrder(_, nonTransientCreateOffset))
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

        val archivalOffset = transactions
          .find(isArchival)
          .map(_.offset)
          .getOrElse(fail("No archival transaction found"))

        transactions
          .filter(isNonConsumingExercise)
          .map(_.offset)
          .foreach(assertOffsetOrder(_, archivalOffset))

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
//                    println(s"TRANSACTIONS: ${transactions.length}")
//                    transactions.filter(isFetch).sortBy(_.offset).foreach { tx =>
//                      val events = tx.eventsById.values.map { x => x.kind}.mkString(" , ")
//                      println(s"EVENTS ${tx.eventsById.size}: ${tx.offset} ${events}")
//                    }

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

            val archivalOffset = transactions
              .find(isArchival)
              .map(_.offset)
              .getOrElse(fail("No archival transaction found"))

            transactions
              .filter(isFetch)
              .map(_.offset)
              .foreach(assertOffsetOrder(_, archivalOffset))
          }
  })

//  test(
//    "RWArchiveVsLookupByKey",
//    "Cannot successfully lookup by key an archived contract",
//    allocate(SingleParty),
//  )(implicit ec => {
//    case Participants(Participant(ledger, alice)) =>
//      val ArchiveAt = 50
//      val Attempts = 100
//      for {
//        contract <- ledger.create(alice, ContractWithChoices(alice))
//        looker <- ledger.create(alice, LookerUpByKey(alice))
//        results <- Future.traverse(1 to Attempts) { attempt =>
//          if (attempt == ArchiveAt) {
//            ledger.exercise(alice, contract.exerciseContractWithChoices_Archive).transform(Success(_))
//          } else {
//            ledger.exercise(alice, looker.exerciseLookup).transform(Success(_))
//          }
//        }
//        transactions <- ledger.transactionTrees(alice)
//      } yield {
//        println(s"TRANSACTIONS: ${transactions.length}")
//        transactions.sortBy(_.offset).foreach { tx =>
//          val events = tx.eventsById.values.map { x => x.kind}.mkString(" , ")
//          println(s"EVENTS ${tx.eventsById.size}: ${tx.offset} ${events}")
//        }
//
////        val ArchivalChoiceName = "ContractWithChoices_Archive"
//        val LookupChoiceName = "Lookup"
//        val successResults: Seq[TransactionTree] = results.collect {
//          case Success(result) => result
//        }
//
////        val archivalTree: TransactionTree = successResults
////          .find(headExerciseChoice(_) == ArchivalChoiceName)
////          .getOrElse(fail(s"Not found successful event named: $ArchivalChoiceName"))
//
//        successResults
//          .filter(headExerciseChoice(_) == LookupChoiceName)
//          .foreach { tree =>
//            tree.eventsById.values.foreach { event =>
//              println(s"${event.getExercised.choice} ${event.getExercised.exerciseResult}")
//            }
//            println("----------------")
////            assertOffsetOrder(tree.offset, archivalTree.offset)
//          }
//        results.filter(_.isFailure).foreach { result =>
//          assertGrpcError(result.failed.get, Status.Code.INVALID_ARGUMENT, "Unknown contract")
//        }
//      }
//  })

  def assertOffsetOrder(a: String, b: String): Unit = {
    if (Bytes.ordering.lt(offsetBytes(a), offsetBytes(b))) ()
    else fail(s"Offset $a is not before $b")
  }

  def offsetBytes(offset: String) = {
    Bytes.fromHexString(Ref.HexString.assertFromString(offset))
  }

}
