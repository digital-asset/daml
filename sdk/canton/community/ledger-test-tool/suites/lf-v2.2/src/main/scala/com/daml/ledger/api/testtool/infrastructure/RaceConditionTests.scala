// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.timer.Delayed
import com.digitalasset.canton.ledger.api.TransactionShape.LedgerEffects

import scala.concurrent.duration.*
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
      party: Party,
      waitBefore: FiniteDuration = WaitBeforeGettingTransactions,
  )(implicit ec: ExecutionContext): Future[Vector[Transaction]] =
    Delayed.by(waitBefore)(()).flatMap(_ => ledger.transactions(LedgerEffects, party))

  def assertTransactionOrder(
      expectedFirst: Transaction,
      expectedSecond: Transaction,
  ): Unit =
    if (expectedFirst.offset < expectedSecond.offset) ()
    else fail(s"""Offset ${expectedFirst.offset} is not before ${expectedSecond.offset}
         |
         |Expected first: ${printTransaction(expectedFirst)}
         |Expected second: ${printTransaction(expectedSecond)}
         |""".stripMargin)

  def printTransaction(transaction: Transaction): String =
    s"""Offset: ${transaction.offset}, number of events: ${transaction.events.size}
       |${transaction.events.map(e => s" -> $e").mkString("\n")}
       |""".stripMargin

}
