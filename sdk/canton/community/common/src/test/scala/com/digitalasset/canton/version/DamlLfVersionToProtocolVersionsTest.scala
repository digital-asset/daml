// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protocol.LfLanguageVersion
import org.scalatest.wordspec.AnyWordSpec

import scala.math.Ordered.orderingToOrdered
import scala.util.Try

class DamlLfVersionToProtocolVersionsTest extends AnyWordSpec with BaseTest {

  val supportedTransactionVersions = LfLanguageVersion.AllV2.filter(_ >= LfLanguageVersion.v2_1)

  "DamlLFVersionToProtocolVersions" should {
    supportedTransactionVersions.foreach { version =>
      s"find the minimum protocol version for $version" in {
        assert(
          Try(
            DamlLfVersionToProtocolVersions.getMinimumSupportedProtocolVersion(version)
          ).isSuccess,
          s"Add $version to damlLfVersionToProtocolVersions Map",
        )

      }
    }
  }
}
