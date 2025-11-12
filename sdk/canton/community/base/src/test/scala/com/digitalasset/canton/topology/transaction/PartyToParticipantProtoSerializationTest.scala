// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.syntax.option.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.{crypto, protocol}
import com.google.protobuf
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** The standard serialization/deserialization scenarios are covered by
  * [[com.digitalasset.canton.version.SerializationDeserializationTest]]
  */
class PartyToParticipantProtoSerializationTest extends AnyWordSpec with Matchers with Inside {

  import PartyToParticipantProtoSerializationTest.*

  "PartyToParticipant.fromProtoV30" should {

    "return the expected error for invalid signing key data" in {
      val protoV30 = protocol.v30.PartyToParticipant(
        party = partyIdString,
        threshold = 1,
        participants = proto.hostingParticipants,
        partySigningKeys = crypto.v30
          .SigningKeysWithThreshold(
            keys = proto.signingKeys,
            threshold = 3,
          )
          .some,
      )

      inside(PartyToParticipant.fromProtoV30(protoV30)) { case Left(InvariantViolation(_, error)) =>
        error should include("Cannot meet threshold")
      }
    }
  }
}

object PartyToParticipantProtoSerializationTest {

  private val partyIdString =
    "test::1220e5abbff1339f99071b5a368712fa006ad3fe8298248952426302624f40244737"
  private val participant1IdString =
    "participant1::12201555721bdafd9ae72f507d21d625f2a30f2f2a63c8652dec1c8d05f04a9328d1"
  private val participant2IdString =
    "participant2::1220951ff48f4e181ad36d0db03e4096d2b59239e275551b809450f7bf2072996887"

  object proto {
    val hostingParticipants: Seq[protocol.v30.PartyToParticipant.HostingParticipant] = Seq(
      proto.hostingParticipant(
        participantUid = participant1IdString
      ),
      proto.hostingParticipant(
        participantUid = participant2IdString,
        onboarding = true,
      ),
    )

    val signingKeys: Seq[crypto.v30.SigningPublicKey] = Seq(
      proto.signingPublicKey(
        "302a300506032b6570032100ed3bf0e036512a32a1d5dfb13a3f7ae7b0d73122fd34421e485871817e3cd425"
      ),
      proto.signingPublicKey(
        "302a300506032b65700321004e67c41ffa42820d593a700a5b491b51354e2c668b84605924b47c98c736ad69"
      ),
    )

    def signingPublicKey(hexString: String): crypto.v30.SigningPublicKey =
      crypto.v30.SigningPublicKey(
        crypto.v30.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO,
        protobuf.ByteString.fromHex(hexString),
        crypto.v30.SigningKeyScheme.SIGNING_KEY_SCHEME_UNSPECIFIED,
        List(
          crypto.v30.SigningKeyUsage.SIGNING_KEY_USAGE_NAMESPACE,
          crypto.v30.SigningKeyUsage.SIGNING_KEY_USAGE_PROTOCOL,
          crypto.v30.SigningKeyUsage.SIGNING_KEY_USAGE_PROOF_OF_OWNERSHIP,
        ),
        crypto.v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519,
      )

    def hostingParticipant(
        participantUid: String,
        onboarding: Boolean = false,
    ): protocol.v30.PartyToParticipant.HostingParticipant =
      protocol.v30.PartyToParticipant.HostingParticipant(
        participantUid = participantUid,
        permission = protocol.v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION,
        onboarding =
          if (onboarding) protocol.v30.PartyToParticipant.HostingParticipant.Onboarding().some
          else None,
      )
  }
}
