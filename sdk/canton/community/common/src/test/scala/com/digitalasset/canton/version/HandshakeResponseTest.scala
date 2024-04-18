// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.sequencing.protocol.HandshakeResponse
import com.digitalasset.canton.sequencing.protocol.HandshakeResponse.Failure
import org.scalatest.wordspec.AnyWordSpec

class HandshakeResponseTest extends AnyWordSpec with BaseTest {

  private val deletedProtocolVersions = Table(
    "deleted protocol version",
    ProtocolVersion.v3,
    ProtocolVersion.v4,
  )

  "HandshakeResponse" should {

    "for a deleted protocol version" should {
      forAll(deletedProtocolVersions) { deletedPV =>
        s"fails deserialization of Success message for pv=$deletedPV" in {
          deletedPV.isDeleted shouldBe true
          val res = HandshakeResponse.fromProtoV0(createSuccessResp(deletedPV))

          res.left.value.message shouldBe (ProtocolVersion.unsupportedErrorMessage(deletedPV))
        }

        s"deserializes Failure message with failure reason stating pv=$deletedPV" in {
          deletedPV.isDeleted shouldBe true
          val res = HandshakeResponse.fromProtoV0(createFailureResp(deletedPV))

          res.value shouldBe Failure(deletedPV, "server unhappy")
        }
      }
    }

    "for a supported protocol version" should {
      forAll(Table("protocol versions", ProtocolVersion.supported *)) { supportedPV =>
        s"deserializes Success message for pv=$supportedPV" in {
          val res = HandshakeResponse.fromProtoV0(createSuccessResp(supportedPV))

          res.value shouldBe HandshakeResponse.Success(supportedPV)
        }

        s"deserializes Failure message with failure reason stating pv=$supportedPV" in {
          val res = HandshakeResponse.fromProtoV0(createFailureResp(supportedPV))

          res.value shouldBe Failure(supportedPV, "server unhappy")
        }
      }
    }
  }

  private def createSuccessResp(deletedPV: ProtocolVersion) = {
    val resp = HandshakeResponse.Success(deletedPV)
    resp.toProtoV0
  }

  private def createFailureResp(deletedPV: ProtocolVersion) = {
    val resp = HandshakeResponse.Failure(deletedPV, "server unhappy")
    resp.toProtoV0
  }

}
