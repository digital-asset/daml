// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.version

import com.digitalasset.canton.data.GeneratorsDataTime
import com.digitalasset.canton.participant.admin.data.{ActiveContract, GeneratorsData}
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, SerializationDeserializationTestHelpers}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SerializationDeserializationTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckPropertyChecks
    with SerializationDeserializationTestHelpers {

  forAll(Table("protocol version", ProtocolVersion.supported *)) { version =>
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
