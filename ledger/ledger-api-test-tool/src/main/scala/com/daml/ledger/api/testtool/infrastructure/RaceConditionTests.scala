// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value.RecordField
import com.daml.ledger.client.binding.Primitive
import com.daml.lf.data.{Bytes, Ref}
import com.daml.timer.Delayed

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

private[testtool] object RaceConditionTests {
  val DefaultRepetitionsNumber: Int = 3
  private val WaitBeforeGettingTransactions: FiniteDuration = 500.millis

  def executeRepeatedlyWithRandomDelay[T](
      numberOfAttempts: Int,
      once: => Future[T],
      repeated: => Future[T],
  )(implicit ec: ExecutionContext): Future[Vector[Try[T]]] = {
    val attempts = (1 to numberOfAttempts).toVector
    Future.traverse(attempts) { attempt =>
      scheduleWithRandomDelay(upTo = 20.millis) { _ =>
        (if (attempt == numberOfAttempts) {
           once
         } else {
           repeated
         }).transform(Success(_))
      }
    }
  }

  private def randomDurationUpTo(limit: FiniteDuration): FiniteDuration =
    scala.util.Random.nextInt(limit.toMillis.toInt).millis

  private def scheduleWithRandomDelay[T](upTo: FiniteDuration)(f: Unit => Future[T])(implicit
      ec: ExecutionContext
  ): Future[T] =
    Delayed.by(randomDurationUpTo(upTo))(()).flatMap(f)

  def transactions(
      ledger: ParticipantTestContext,
      party: Primitive.Party,
      waitBefore: FiniteDuration = WaitBeforeGettingTransactions,
  )(implicit ec: ExecutionContext): Future[Vector[TransactionTree]] =
    Delayed.by(waitBefore)(()).flatMap(_ => ledger.transactionTrees(party))

  def assertTransactionOrder(
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

  def printTransaction(transactionTree: TransactionTree): String = {
    s"""Offset: ${transactionTree.offset}, number of events: ${transactionTree.eventsById.size}
       |${transactionTree.eventsById.values.map(e => s" -> $e").mkString("\n")}
       |""".stripMargin
  }

  private def offsetLessThan(a: String, b: String): Boolean =
    Bytes.ordering.lt(offsetBytes(a), offsetBytes(b))

  private def offsetBytes(offset: String): Bytes = {
    Bytes.fromHexString(Ref.HexString.assertFromString(offset))
  }

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

    def isContractLookup(success: Boolean)(tx: TransactionTree): Boolean =
      tx.containsEvent { event =>
        isCreated(RaceTests.LookupResult.TemplateName)(event) &&
        event.getCreated.getCreateArguments.fields.exists(isFoundContractField(found = success))
      }
  }

  private object RaceTests {
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
