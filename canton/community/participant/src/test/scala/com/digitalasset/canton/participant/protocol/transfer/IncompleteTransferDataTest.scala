// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.IncompleteTransferData.TransferEventGlobalOffset
import org.scalatest.wordspec.AnyWordSpec

class IncompleteTransferDataTest extends AnyWordSpec with BaseTest {
  "TransferEventGlobalOffset" should {
    "be create from (queryOffset, transferOutGlobalOffset, transferInGlobalOffset)" in {
      import TransferEventGlobalOffset.create
      import IncompleteTransferData.{
        TransferInEventGlobalOffset as In,
        TransferOutEventGlobalOffset as Out,
      }
      import scala.language.implicitConversions

      implicit def toGlobalOffset(i: Int) = GlobalOffset.tryFromLong(i.toLong)

      create(9, Some(10), None).left.value shouldBe a[String] // No event emitted
      create(10, Some(10), None).value shouldBe Out(10)
      create(11, Some(10), None).value shouldBe Out(10)
      create(11, Some(10), Some(12)).value shouldBe Out(10)

      create(19, None, Some(20)).left.value shouldBe a[String] // No event emitted
      create(20, None, Some(20)).value shouldBe In(20)
      create(21, None, Some(20)).value shouldBe In(20)
      create(21, Some(22), Some(20)).value shouldBe In(20)

      create(11, Some(10), Some(11)).left.value shouldBe a[String] // Both events emitted
      create(11, Some(11), Some(10)).left.value shouldBe a[String] // Both events emitted
    }
  }

}
