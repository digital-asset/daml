// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protocol.LfSerializationVersion
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class LfSerializationVersionToProtocolVersionsTest extends AnyWordSpec with BaseTest {

  val supportedSerializationVersions =
    List(LfSerializationVersion.V1, LfSerializationVersion.VDev)

  "DamlLFVersionToProtocolVersions" should {
    supportedSerializationVersions.foreach { version =>
      s"find the minimum protocol version for $version" in {
        assert(
          Try(
            LfSerializationVersionToProtocolVersions.getMinimumSupportedProtocolVersion(version)
          ).isSuccess,
          s"Add $version to damlLfVersionToProtocolVersions Map",
        )

      }
    }
  }
}
