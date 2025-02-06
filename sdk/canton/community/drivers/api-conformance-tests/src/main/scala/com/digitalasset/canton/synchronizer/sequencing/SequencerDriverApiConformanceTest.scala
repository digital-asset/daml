// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing

import com.digitalasset.canton.synchronizer.block.RawLedgerBlock.RawBlockEvent.Send
import com.digitalasset.canton.synchronizer.block.{RawLedgerBlock, SequencerDriver}
import com.digitalasset.canton.synchronizer.sequencing.BaseSequencerDriverApiTest.CompletionTimeout
import com.digitalasset.canton.util.Thereafter.syntax.*
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

    "send an event and get it back" in {
      val driver = createDriver()
      for {
        _ <- driver.send(ByteString.copyFromUtf8("event"), "submissionId", "senderId")
        _ <- pullSends(driver)
      } yield succeed
    }

    "get same send event after resubscribing" in {
      val driver = createDriver()
      for {
        _ <- driver.send(ByteString.copyFromUtf8("event"), "submissionId", "senderId")
        block <- pullSends(driver)
        driver2 = createDriver(firstBlockHeight = Some(block.blockHeight))
        block2 <- pullSends(driver2)
      } yield block shouldBe block2
    }

    "do not throw on acknowledge" in {
      val driver = createDriver()
      // acknowledge is unsupported for some drivers
      driver
        .acknowledge(ByteString.copyFromUtf8("ack"))
        .thereafter(_ => driver.close())
        .map(_ => succeed)
    }

    "get health" in {
      val driver = createDriver()
      driver.health.thereafter(_ => driver.close()).map(_.isActive shouldBe true)
    }
  }

  private def pullSends(driver: SequencerDriver) =
    driver
      .subscribe()
      .filter(blockContainsSendEvent)
      .completionTimeout(CompletionTimeout)
      .runWith(Sink.head)
      .thereafter(_ => driver.close())

  private def blockContainsSendEvent(rawBlock: RawLedgerBlock) =
    rawBlock.events.exists(tracedEv =>
      tracedEv.value match {
        case Send(request, _) =>
          request.toStringUtf8 == "event" && tracedEv.traceContext == traceContext
        case _ => false
      }
    )
}
