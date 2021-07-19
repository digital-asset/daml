// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.inmemory

import akka.stream.ThrottleMode
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.offset.Offset
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class LedgerEntriesSpec
    extends AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with Inspectors {

  case class Error(msg: String)

  case class Transaction(content: String)

  val NO_OF_MESSAGES = 10000
  val NO_OF_SUBSCRIBERS = 50

  private def genTransactions() = (1 to NO_OF_MESSAGES).map { i =>
    if (Random.nextBoolean())
      Right(Transaction(i.toString))
    else
      Left(Error(i.toString))
  }

  "LedgerEntries" should {

    "store new blocks and a late subscriber can read them" in {
      val ledger = new LedgerEntries[Either[Error, Transaction]](_.toString)
      val transactions = genTransactions()

      transactions.foreach(t => ledger.publish(t))

      val sink =
        Flow[(Offset, Either[Error, Transaction])]
          .take(NO_OF_MESSAGES.toLong)
          .toMat(Sink.seq)(Keep.right)

      val blocksF = ledger.getSource(None, None).runWith(sink)

      blocksF.map { blocks =>
        val readTransactions = blocks.collect { case (_, transaction) => transaction }
        readTransactions shouldEqual transactions
      }
    }

    "store new blocks while multiple subscribers are reading them with different pace" in {
      val transactions = genTransactions()

      val ledger = new LedgerEntries[Either[Error, Transaction]](_.toString)

      val publishRate = NO_OF_MESSAGES / 10

      val blocksInStream =
        Source(transactions)
          .throttle(publishRate, 100.milliseconds, publishRate, ThrottleMode.shaping)
          .to(Sink.foreach { t =>
            ledger.publish(t)
            ()
          })

      def subscribe() = {
        val subscribeRate = NO_OF_MESSAGES / (Random.nextInt(100) + 1)
        ledger
          .getSource(None, None)
          .runWith(
            Flow[(Offset, Either[Error, Transaction])]
              .throttle(subscribeRate, 100.milliseconds, subscribeRate, ThrottleMode.shaping)
              .take(NO_OF_MESSAGES.toLong)
              .toMat(Sink.seq)(Keep.right)
          )
      }

      val readBlocksF = Future.sequence((1 to NO_OF_SUBSCRIBERS).map(_ => subscribe()))
      blocksInStream.run()

      readBlocksF.map { readBlocksForAll =>
        forAll(readBlocksForAll) { readBlocks =>
          val readTransactions = readBlocks.collect { case (_, transaction) => transaction }
          readTransactions shouldEqual transactions
        }
      }
    }
  }
}
