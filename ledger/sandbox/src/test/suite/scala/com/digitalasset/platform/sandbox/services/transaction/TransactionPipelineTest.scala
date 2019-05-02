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

      val resF = eventStream.untilRequired(50).runWith(Sink.seq)
      resF.map { res =>
        res.size shouldEqual 10
        res.last.offset shouldEqual "45"
      }
    }
  }

}
