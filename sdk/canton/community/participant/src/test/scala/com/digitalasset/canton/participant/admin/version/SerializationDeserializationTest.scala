// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.version

import com.digitalasset.canton.data.GeneratorsDataTime
import com.digitalasset.canton.participant.admin.data.{ActiveContract, GeneratorsData}
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.ProtocolVersion.{v3, v4}
import com.digitalasset.canton.{BaseTest, SerializationDeserializationTestHelpers}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SerializationDeserializationTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckPropertyChecks
    with SerializationDeserializationTestHelpers {

  // Continue to test the (de)serialization for protocol version 3 and 4 to support the upgrade to 2.x LTS:
  // A participant node will contain data that was serialized using an old a protocol version which then may
  // get deserialized during the upgrade. For example, there may be still contracts which use an old contract ID
  // scheme.
  forAll(Table("protocol version", (ProtocolVersion.supported ++ List(v3, v4)) *)) { version =>
    val generatorsDataTime = new GeneratorsDataTime()
    val generatorsProtocol = new GeneratorsProtocol(version, generatorsDataTime)
    val generatorsData = new GeneratorsData(version, generatorsProtocol)
    import generatorsData.*

    s"Serialization and deserialization methods using protocol version $version" should {
      "compose to the identity" in {
        testProtocolVersioned(ActiveContract)
      }
    }
  }
}
