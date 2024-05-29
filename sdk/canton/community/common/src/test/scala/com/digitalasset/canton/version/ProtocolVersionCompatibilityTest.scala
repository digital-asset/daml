// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.version.ProtocolVersionCompatibility.canClientConnectToServer
import org.scalatest.wordspec.AnyWordSpec

class ProtocolVersionCompatibilityTest extends AnyWordSpec with BaseTest {
  "ProtocolVersionCompatibility" should {
    "version check" should {
      "be successful for matching unstable versions" in {
        canClientConnectToServer(
          clientSupportedVersions = Seq(testedProtocolVersion, ProtocolVersion.dev),
          server = ProtocolVersion.dev,
          None,
        ) shouldBe Right(())
      }

      "be successful for matching stable versions" in {
        canClientConnectToServer(
          clientSupportedVersions = Seq(testedProtocolVersion),
          server = testedProtocolVersion,
          None,
        ) shouldBe Right(())
      }

      "fail for incompatible versions" in {
        canClientConnectToServer(
          clientSupportedVersions = Seq(ProtocolVersion.v5),
          server = ProtocolVersion.v6,
          None,
        ).left.value shouldBe (VersionNotSupportedError(
          server = ProtocolVersion.v6,
          clientSupportedVersions = Seq(ProtocolVersion.v5),
        ))
      }

      "fail if the domain requires a lower version than the participant's minimum version" in {
        canClientConnectToServer(
          clientSupportedVersions = Seq(ProtocolVersion.v5),
          server = ProtocolVersion.v4,
          Some(ProtocolVersion.v5),
        ).left.value shouldBe (MinProtocolError(
          server = ProtocolVersion(4),
          clientMinimumProtocolVersion = Some(ProtocolVersion.v5),
          clientSupportsRequiredVersion = false,
        ))
      }
    }
  }
}
