// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.admin.data.{ActiveContractOld, GeneratorsData}
import com.digitalasset.canton.version.{
  CommonGenerators,
  ProtocolVersion,
  SerializationDeserializationTestHelpers,
}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SerializationDeserializationTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckPropertyChecks
    with SerializationDeserializationTestHelpers {

  forAll(Table("protocol version", ProtocolVersion.supported*)) { version =>
    val generators = new CommonGenerators(version)
    val generatorsAdminData = new GeneratorsData(version, generators.protocol, generators.topology)
    import generatorsAdminData.*

    s"Serialization and deserialization methods using protocol version $version" should {
      "compose to the identity" in {
        test(ActiveContractOld, version)
      }
    }
  }
}
