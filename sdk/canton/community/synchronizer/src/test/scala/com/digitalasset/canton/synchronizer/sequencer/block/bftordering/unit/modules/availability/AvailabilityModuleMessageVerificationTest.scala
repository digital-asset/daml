// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.availability

import cats.syntax.either.*
import com.digitalasset.canton.crypto.{Signature, SignatureCheckError}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.FingerprintKeyId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftKeyId,
  BftNodeId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.MessageAuthorizer
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

      "drop it" when {
        "unauthorized" in {
          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val cryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]
          val messageAuthorizer = mock[MessageAuthorizer]
          when(messageAuthorizer.isAuthorized(any[BftNodeId], any[BftKeyId])) thenReturn false
          val availability = createAvailability[ProgrammableUnitTestEnv](
            cryptoProvider = cryptoProvider,
            customMessageAuthorizer = Some(messageAuthorizer),
          )

          val underlyingMessage = mock[Availability.RemoteProtocolMessage]
          val signedMessage = underlyingMessage.fakeSign
          when(underlyingMessage.from) thenReturn Node0

          assertLogs(
            availability.receive(Availability.UnverifiedProtocolMessage(signedMessage)),
            (logEntry: LogEntry) => {
              logEntry.level shouldBe Level.INFO
              logEntry.message should include(
                "it cannot be verified in the currently known dissemination topology"
              )
            },
          )

          verify(messageAuthorizer).isAuthorized(
            Node0,
            FingerprintKeyId.toBftKeyId(Signature.noSignature.signedBy),
          )

          context.runPipedMessages() shouldBe empty
        }
      }

      "verify it and if okay it should send the underlying message to self" in {
        implicit val context
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext
        val cryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]
        val availability = createAvailability[ProgrammableUnitTestEnv](
          cryptoProvider = cryptoProvider,
          otherNodes = Set(Node1),
          otherNodesCustomKeys =
            Map(Node1 -> FingerprintKeyId.toBftKeyId(Signature.noSignature.signedBy)),
        )

        val underlyingMessage = mock[Availability.RemoteProtocolMessage]
        when(underlyingMessage.from) thenReturn Node1

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
          cryptoProvider = cryptoProvider,
          otherNodes = Set(Node1),
          otherNodesCustomKeys =
            Map(Node1 -> FingerprintKeyId.toBftKeyId(Signature.noSignature.signedBy)),
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
