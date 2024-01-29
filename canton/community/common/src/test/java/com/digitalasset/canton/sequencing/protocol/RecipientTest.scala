// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.topology.{MediatorId, MediatorRef, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.{BaseTest, ProtoDeserializationError}
import org.scalatest.wordspec.AnyWordSpec

class RecipientTest extends AnyWordSpec with BaseTest {
  private val memberRecipient = Recipient(ParticipantId("participant1"))

  "recipient test serialization" should {
    "be able to convert back and forth" in {
      Recipient.fromProtoPrimitive(
        memberRecipient.toProtoPrimitive,
        "recipient",
      ) shouldBe Right(memberRecipient)
    }

    "act sanely on invalid inputs" in {
      forAll(
        Seq(
          "nothing valid",
          "",
          "::",
          "INV::invalid",
          "POP",
          "POP::incomplete",
          "POP::incomplete::",
          "POP,,alice::party",
          "MOD::99312312::",
          "MOD::99312312::gibberish",
          "MOD::not-a-number",
          "MOD::99312312993123129931231299312312",
        )
      ) { str =>
        Recipient
          .fromProtoPrimitive(str, "recipient")
          .left
          .value shouldBe a[ProtoDeserializationError]
      }
    }

    "work on MediatorRecipient" in {
      val mediatorMemberMR =
        MediatorRef(MediatorId(UniqueIdentifier.tryCreate("mediator", "fingerprint")))

      MediatorRef.fromProtoPrimitive(
        mediatorMemberMR.toProtoPrimitive,
        "mediator",
      ) shouldBe (Right(mediatorMemberMR))

      MediatorRef
        .fromProtoPrimitive(
          memberRecipient.toProtoPrimitive, // a participant
          "mediator",
        )
        .left
        .value shouldBe a[ProtoDeserializationError]
    }
  }
}
