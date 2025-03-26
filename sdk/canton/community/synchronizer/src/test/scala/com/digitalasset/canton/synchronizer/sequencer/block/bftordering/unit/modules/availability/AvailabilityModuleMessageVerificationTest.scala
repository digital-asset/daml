// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.availability

import cats.syntax.either.*
import com.digitalasset.canton.crypto.SignatureCheckError
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.*
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

class AvailabilityModuleMessageVerificationTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with AvailabilityModuleTestUtils {

  "The availability module" when {

    "it receives UnverifiedProtocolMessage" should {

      "verify it and if okay it should send the underlying message to self" in {
        implicit val context
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext
        val cryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]
        val availability = createAvailability[ProgrammableUnitTestEnv](
          cryptoProvider = cryptoProvider
        )

        val underlyingMessage = mock[Availability.RemoteProtocolMessage]
        val signedMessage = underlyingMessage.fakeSign

        when(
          cryptoProvider.verifySignedMessage(
            signedMessage,
            AuthenticatedMessageType.BftSignedAvailabilityMessage,
          )
        ) thenReturn (() => Either.unit)

        availability.receive(Availability.UnverifiedProtocolMessage(signedMessage))

        context.runPipedMessages() shouldBe Seq(underlyingMessage)
      }

      "verify it and if not okay it should drop it" in {
        implicit val context
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext
        val cryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]
        val availability = createAvailability[ProgrammableUnitTestEnv](
          cryptoProvider = cryptoProvider
        )

        val underlyingMessage = mock[Availability.RemoteProtocolMessage]
        val signedMessage = underlyingMessage.fakeSign
        val signatureCheckError = mock[SignatureCheckError]

        when(underlyingMessage.from) thenReturn Node1

        when(
          cryptoProvider.verifySignedMessage(
            signedMessage,
            AuthenticatedMessageType.BftSignedAvailabilityMessage,
          )
        ) thenReturn (() => Left(signatureCheckError))

        availability.receive(Availability.UnverifiedProtocolMessage(signedMessage))

        assertLogs(
          context.runPipedMessages() shouldBe Seq(Availability.NoOp),
          (logEntry: LogEntry) => {
            logEntry.level shouldBe Level.INFO
            logEntry.message should include("Skipping message since we can't verify signature")
          },
        )
      }
    }
  }
}
