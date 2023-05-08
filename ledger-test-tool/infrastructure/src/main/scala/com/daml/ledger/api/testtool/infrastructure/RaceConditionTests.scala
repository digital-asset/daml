// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.transaction.TransactionTree
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
}
