// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.daml.lf.transaction.TransactionVersion
import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

import scala.math.Ordered.orderingToOrdered
import scala.util.Try

class DamlLfVersionToProtocolVersionsTest extends AnyWordSpec with BaseTest {

  val supportedTransactionVersions = TransactionVersion.All.filter(_ >= TransactionVersion.V31)

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
