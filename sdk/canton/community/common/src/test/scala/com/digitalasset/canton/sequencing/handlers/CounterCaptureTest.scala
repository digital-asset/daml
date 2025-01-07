// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import cats.syntax.either.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.sequencing.{SequencerTestUtils, SerializedEventHandler}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

final case class HandlerError(message: String)

class CounterCaptureTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  type TestEventHandler = SerializedEventHandler[HandlerError]

  "CounterCapture" should {
    "return initial value if we've not successfully processed an event" in {
      val counterCapture = new CounterCapture(SequencerCounter(1), loggerFactory)

      counterCapture.counter shouldBe SequencerCounter(1)
    }

    "update the counter when we successfully process an event" in {
      val counterCapture = new CounterCapture(SequencerCounter(1), loggerFactory)
      val handler: TestEventHandler = _ => FutureUnlessShutdown.pure(Either.unit)
      val capturingHandler = counterCapture(handler)

      val fut = capturingHandler(
        OrdinarySequencedEvent(
          sign(SequencerTestUtils.mockDeliver(sc = 42))
        )(traceContext)
      )

      counterCapture.counter shouldBe SequencerCounter(42)
      fut.futureValueUS shouldBe Either.unit
    }

    "not update the counter when the handler fails" in {
      val counterCapture = new CounterCapture(SequencerCounter(1), loggerFactory)
      val ex = new RuntimeException
      val handler: TestEventHandler = _ => FutureUnlessShutdown.failed(ex)
      val capturingHandler = counterCapture(handler)

      val fut = capturingHandler(
        OrdinarySequencedEvent(
          sign(SequencerTestUtils.mockDeliver(sc = 42))
        )(traceContext)
      )

      counterCapture.counter shouldBe SequencerCounter(1)
      fut.failed.futureValueUS shouldBe ex
    }
  }

  private def sign[A <: HasCryptographicEvidence](content: A): SignedContent[A] =
    SignedContent(content, SymbolicCrypto.emptySignature, None, testedProtocolVersion)
}
