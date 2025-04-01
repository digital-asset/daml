// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import cats.syntax.either.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.sequencing.{SequencerTestUtils, SerializedEventHandler}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

final case class HandlerError(message: String)

class EventTimestampCaptureTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  type TestEventHandler = SerializedEventHandler[HandlerError]

  "EventTimestampCapture" should {
    "return initial value if we've not successfully processed an event" in {
      (new EventTimestampCapture(initial = None, loggerFactory)).latestEventTimestamp shouldBe None
      (new EventTimestampCapture(
        initial = Some(CantonTimestamp.ofEpochSecond(42L)),
        loggerFactory,
      )).latestEventTimestamp shouldBe Some(CantonTimestamp.ofEpochSecond(42L))
    }

    "update the timestamp when we successfully process an event" in {
      val timestampCapture =
        new EventTimestampCapture(Some(CantonTimestamp.ofEpochSecond(2L)), loggerFactory)
      val handler: TestEventHandler = _ => FutureUnlessShutdown.pure(Either.unit)
      val capturingHandler = timestampCapture(handler)

      val fut = capturingHandler(
        OrdinarySequencedEvent(
          sign(
            SequencerTestUtils.mockDeliver(sc = 42, timestamp = CantonTimestamp.ofEpochSecond(42))
          )
        )(traceContext)
      )

      timestampCapture.latestEventTimestamp shouldBe Some(CantonTimestamp.ofEpochSecond(42))
      fut.futureValueUS shouldBe Either.unit
    }

    "not update the timestamp when the handler fails" in {
      val timestampCapture =
        new EventTimestampCapture(Some(CantonTimestamp.ofEpochSecond(2L)), loggerFactory)
      val ex = new RuntimeException
      val handler: TestEventHandler = _ => FutureUnlessShutdown.failed(ex)
      val capturingHandler = timestampCapture(handler)

      val fut = capturingHandler(
        OrdinarySequencedEvent(
          sign(
            SequencerTestUtils.mockDeliver(sc = 42, timestamp = CantonTimestamp.ofEpochSecond(42))
          )
        )(traceContext)
      )

      timestampCapture.latestEventTimestamp shouldBe Some(CantonTimestamp.ofEpochSecond(2L))
      fut.failed.futureValueUS shouldBe ex
    }
  }

  private def sign[A <: HasCryptographicEvidence](content: A): SignedContent[A] =
    SignedContent(content, SymbolicCrypto.emptySignature, None, testedProtocolVersion)
}
