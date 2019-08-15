// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.transaction

import akka.stream.ThrottleMode
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  MockMessages,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.platform.apitesting.{MultiLedgerFixture, TestCommands, TestIdsGenerator}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import scala.concurrent.{Await, Future}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any"
  ))
class TransactionBackpressureIT
    extends AsyncWordSpec
    with Matchers
    with ScalaFutures
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundAll
    with MultiLedgerFixture {

  protected val testCommands = new TestCommands(config)
  protected val testIdsGenerator = new TestIdsGenerator(config)

  override def timeLimit: Span = scaled(300.seconds)

  override protected def config: Config = Config.default

  override protected def parallelExecution: Boolean = false

  private val transactionFilter = MockMessages.transactionFilter

  "The transaction service when serving multiple subscriptions" should {

    "handle back pressure" in allFixtures { ctx =>
      val noOfCommands = 1000
      val noOfSubscriptions = 10

      val transactionClient = ctx.transactionClient
      val commandClient = Await.result(ctx.commandClient(), 5.seconds)

      def sendCommands() =
        Source(1 to noOfCommands)
          .throttle(10, 1.second)
          .mapAsync(10)(i =>
            commandClient.submitSingleCommand(testCommands
              .oneKbCommandRequest(ctx.ledgerId, testIdsGenerator.testCommandId(s"command-$i"))))
          .runWith(Sink.ignore)

      def subscribe(begin: LedgerOffset, rate: Int) =
        transactionClient
          .getTransactions(begin, None, transactionFilter)
          .take(noOfCommands.toLong)
          .throttle(rate, 1.second, noOfCommands, ThrottleMode.shaping)
          .runWith(Sink.seq)

      def startSubscriptions(begin: LedgerOffset) = {
        (1 to noOfSubscriptions).map(i => subscribe(begin, i * 600))
      }

      val transactionsF = for {
        currentEnd <- transactionClient.getLedgerEnd
        _ <- sendCommands()
        transactions <- Future.sequence(startSubscriptions(currentEnd.getOffset))
      } yield transactions

      transactionsF map { transactions =>
        transactions
          .sliding(2, 1)
          .foreach { slide =>
            slide(0) should be(slide(1))
          }

        Succeeded
      }
    }
  }
}
