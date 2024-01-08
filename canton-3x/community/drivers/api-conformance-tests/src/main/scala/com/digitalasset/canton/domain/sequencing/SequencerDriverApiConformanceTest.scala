// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.domain.sequencing

import com.digitalasset.canton.domain.block.RawLedgerBlock
import com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.{AddMember, Send}
import com.digitalasset.canton.domain.sequencing.BaseSequencerDriverApiTest.CompletionTimeout
import com.google.protobuf.ByteString
import org.apache.pekko.stream.scaladsl.Sink

abstract class SequencerDriverApiConformanceTest[ConfigType]
    extends BaseSequencerDriverApiTest[ConfigType] {

  "Sequencer driver" should {
    "get an empty sequence of admin services" in {
      // we can close the driver, as we need just the class instance
      val driver = createDriver()
      driver.close()
      // so far there are no admin services
      driver.adminServices shouldBe Seq.empty
    }

    "register a member and get it back" in {
      val driver = createDriver()
      for {
        _ <- driver.registerMember("member")
        _ <- driver
          .subscribe()
          .filter { rawBlock =>
            rawBlock.events.exists(tracedEv =>
              tracedEv.value match {
                case AddMember(member) =>
                  member == "member" && tracedEv.traceContext == traceContext
                case _ => false
              }
            )
          }
          .completionTimeout(CompletionTimeout)
          .runWith(Sink.head)
          .andThen(_ => driver.close())
      } yield succeed
    }

    "send an event and get it back" in {
      val driver = createDriver()
      for {
        _ <- driver.send(ByteString.copyFromUtf8("event"))
        _ <- driver
          .subscribe()
          .filter { rawBlock =>
            rawBlock.events.exists(tracedEv =>
              tracedEv.value match {
                case Send(request, _) =>
                  request.toStringUtf8 == "event" && tracedEv.traceContext == traceContext
                case _ => false
              }
            )
          }
          .completionTimeout(CompletionTimeout)
          .runWith(Sink.head)
          .andThen(_ => driver.close())
      } yield succeed
    }

    "get same send event after resubscribing" in {
      val driver = createDriver()
      def blockContainsSendEvent(rawBlock: RawLedgerBlock) = rawBlock.events.exists(tracedEv =>
        tracedEv.value match {
          case Send(request, _) =>
            request.toStringUtf8 == "event" && tracedEv.traceContext == traceContext
          case _ => false
        }
      )
      for {
        _ <- driver.send(ByteString.copyFromUtf8("event"))
        block <- driver
          .subscribe()
          .filter(blockContainsSendEvent)
          .completionTimeout(CompletionTimeout)
          .runWith(Sink.head)
          .andThen(_ => driver.close())
        driver2 = createDriver(firstBlockHeight = Some(block.blockHeight))
        block2 <- driver2
          .subscribe()
          .filter(blockContainsSendEvent)
          .completionTimeout(CompletionTimeout)
          .runWith(Sink.head)
          .andThen(_ => driver2.close())
      } yield block shouldBe (block2)
    }

    "do not throw on acknowledge" in {
      val driver = createDriver()
      // acknowledge is unsupported for some drivers
      driver
        .acknowledge(ByteString.copyFromUtf8("ack"))
        .andThen(_ => driver.close())
        .map(_ => succeed)
    }

    "get health" in {
      val driver = createDriver()
      driver.health.andThen(_ => driver.close()).map(_.isActive shouldBe true)
    }
  }
}
