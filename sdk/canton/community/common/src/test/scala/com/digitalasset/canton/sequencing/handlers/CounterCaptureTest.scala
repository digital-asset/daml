// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.sequencing.{SequencerTestUtils, SerializedEventHandler}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

final case class HandlerError(message: String)

class CounterCaptureTest extends AnyWordSpec with BaseTest {
  type TestEventHandler = SerializedEventHandler[HandlerError]

  "CounterCapture" should {
    "return initial value if we've not successfully processed an event" in {
      val counterCapture = new CounterCapture(SequencerCounter(1), loggerFactory)

      counterCapture.counter shouldBe SequencerCounter(1)
    }

    "update the counter when we successfully process an event" in {
      val counterCapture = new CounterCapture(SequencerCounter(1), loggerFactory)
      val handler: TestEventHandler = _ => Future.successful(Right(()))
      val capturingHandler = counterCapture(handler)

      val fut = capturingHandler(
        OrdinarySequencedEvent(
          sign(SequencerTestUtils.mockDeliver(sc = 42))
        )(traceContext)
      )

      counterCapture.counter shouldBe SequencerCounter(42)
      fut.futureValue shouldBe Right(())
    }

    "not update the counter when the handler fails" in {
      val counterCapture = new CounterCapture(SequencerCounter(1), loggerFactory)
      val ex = new RuntimeException
      val handler: TestEventHandler = _ => Future.failed(ex)
      val capturingHandler = counterCapture(handler)

      val fut = capturingHandler(
        OrdinarySequencedEvent(
          sign(SequencerTestUtils.mockDeliver(sc = 42))
        )(traceContext)
      )

      counterCapture.counter shouldBe SequencerCounter(1)
      fut.failed.futureValue shouldBe ex
    }
  }

  private def sign[A <: HasCryptographicEvidence](content: A): SignedContent[A] =
    SignedContent(content, SymbolicCrypto.emptySignature, None, testedProtocolVersion)
}
