// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.sequencing.protocol.HandshakeRequest
import org.scalatest.wordspec.AnyWordSpec

class HandshakeRequestTest extends AnyWordSpec with BaseTest {

  private val deletedProtocolVersions = Table(
    "deleted protocol version",
    ProtocolVersion.v3,
    ProtocolVersion.v4,
  )

  private val supportedProtocolVersions =
    Table("supported protocol versions", ProtocolVersion.supported *)

  "HandshakeRequest" should {

    "for a deleted protocol version" should {
      forAll(deletedProtocolVersions) { deletedPV =>
        s"fail deserialization for client pv=$deletedPV " in {
          deletedPV.isDeleted shouldBe true
          val req = HandshakeRequest(Seq(deletedPV), None)
          val res = HandshakeRequest.fromProtoV0(req.toProtoV0)

          res.left.value.message shouldBe ProtocolVersion.unsupportedErrorMessage(deletedPV)
        }
      }

      forAll(deletedProtocolVersions) { deletedPV =>
        s"fail deserialization for minimal pv=$deletedPV" in {
          deletedPV.isDeleted shouldBe true
          val req =
            HandshakeRequest(Seq(testedProtocolVersion), minimumProtocolVersion = Some(deletedPV))
          val res = HandshakeRequest.fromProtoV0(req.toProtoV0)

          res.left.value.message shouldBe ProtocolVersion.unsupportedErrorMessage(deletedPV)
        }
      }
    }

    "for stable and unstable versions" should {

      forAll(supportedProtocolVersions) { version =>
        s"deserialize for client pv=$version" in {
          val req = HandshakeRequest(Seq(version), None)
          val res = HandshakeRequest.fromProtoV0(req.toProtoV0)

          res.value shouldBe req
        }
      }

      forAll(supportedProtocolVersions) { version =>
        s"deserialize for minimal pv=$version" in {
          val req = HandshakeRequest(Seq(version), Some(version))
          val res = HandshakeRequest.fromProtoV0(req.toProtoV0)

          res.value shouldBe req
        }
      }
    }

  }
}
