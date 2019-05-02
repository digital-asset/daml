// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.transaction

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.ledger.backend.api.v1.LedgerSyncEvent
import com.digitalasset.ledger.backend.api.v1.LedgerSyncEvent.Heartbeat
import org.scalatest.{AsyncWordSpec, Matchers}
import TransactionPipeline._
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll

class TransactionPipelineTest extends AsyncWordSpec with Matchers with AkkaBeforeAndAfterAll {

  "Transaction Pipeline" should {

    "stop consuming an event stream without gaps at given ceiling" in {
      val eventStream: Source[LedgerSyncEvent, NotUsed] =
        Source((0 to 100).map(o => Heartbeat(Instant.now(), o.toString)))

      val resF = eventStream.untilRequired(50).runWith(Sink.seq)
      resF.map { res =>
        res.size shouldEqual 50
        res.last.offset shouldEqual "49"
      }
    }

    "stop consuming an event stream with gaps at given ceiling" in {
      val eventStream: Source[LedgerSyncEvent, NotUsed] =
        Source(Range(0, 100, 5)).map(o => Heartbeat(Instant.now(), o.toString))

      for {
        res1 <- eventStream.untilRequired(50).runWith(Sink.seq)
        res2 <- eventStream.untilRequired(51).runWith(Sink.seq)
        res3 <- eventStream.untilRequired(52).runWith(Sink.seq)
        res4 <- eventStream.untilRequired(53).runWith(Sink.seq)
        res5 <- eventStream.untilRequired(54).runWith(Sink.seq)
        res6 <- eventStream.untilRequired(55).runWith(Sink.seq)
        res7 <- eventStream.untilRequired(56).runWith(Sink.seq)
      } yield {
        res1.last.offset shouldEqual "45"
        res2.last.offset shouldEqual "50"
        res3.last.offset shouldEqual "50"
        res4.last.offset shouldEqual "50"
        res5.last.offset shouldEqual "50"
        res6.last.offset shouldEqual "50"
        res7.last.offset shouldEqual "55"
      }
    }

  }

}
