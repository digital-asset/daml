// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class PhysicalSynchronizerIdTest extends AnyWordSpec with EitherValues with Matchers {
  "PhysicalSynchronizerId" should {
    "parsed from string" in {
      val namespace: Namespace = Namespace(Fingerprint.tryFromString("default"))
      val lsid1: SynchronizerId = SynchronizerId(UniqueIdentifier.tryCreate("da", namespace))
      val lsid2: SynchronizerId = SynchronizerId(UniqueIdentifier.tryCreate("da-second", namespace))
      val pv = ProtocolVersion.latest

      val str1 = s"da::default::${pv.toString}-0"
      val str2 = s"da-second::default::${pv.toString}-1"

      PhysicalSynchronizerId.fromString(str1).value shouldBe PhysicalSynchronizerId(
        lsid1,
        pv,
        NonNegativeInt.zero,
      )
      PhysicalSynchronizerId.fromString(str2).value shouldBe PhysicalSynchronizerId(
        lsid2,
        pv,
        NonNegativeInt.one,
      )
    }

    "be properly ordered" in {
      val namespace: Namespace = Namespace(Fingerprint.tryFromString("default"))
      val lsid: SynchronizerId = SynchronizerId(UniqueIdentifier.tryCreate("da", namespace))

      val psid_latest_0 =
        PhysicalSynchronizerId(lsid, ProtocolVersion.latest, serial = NonNegativeInt.zero)
      val psid_latest_1 =
        PhysicalSynchronizerId(lsid, ProtocolVersion.latest, serial = NonNegativeInt.one)
      val psid_dev_2 =
        PhysicalSynchronizerId(lsid, ProtocolVersion.dev, serial = NonNegativeInt.two)

      val inCorrectOrder = List(psid_latest_0, psid_latest_1, psid_dev_2)
      Random.shuffle(inCorrectOrder).sorted shouldBe inCorrectOrder

      val inCorrectOptionOrder = None :: inCorrectOrder.map(Some(_))
      Random.shuffle(inCorrectOptionOrder).sorted shouldBe inCorrectOptionOrder

    }
  }
}
