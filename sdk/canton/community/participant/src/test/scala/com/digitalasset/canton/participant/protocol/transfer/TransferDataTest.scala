// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.transfer.TransferData.*
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

class TransferDataTest extends AnyWordSpec with Matchers with EitherValues {
  private implicit def toGlobalOffset(i: Long): GlobalOffset = GlobalOffset.tryFromLong(i)

  "TransferData.TransferGlobalOffset" should {
    val out1 = TransferOutGlobalOffset(1)
    val out2 = TransferOutGlobalOffset(2)

    val in1 = TransferInGlobalOffset(1)
    val in2 = TransferInGlobalOffset(2)

    val outIn12 = TransferGlobalOffsets.create(1, 2).value
    val outIn21 = TransferGlobalOffsets.create(2, 1).value

    "be mergeable (TransferOutGlobalOffset)" in {
      out1.merge(out1).value shouldBe out1
      out1.merge(out2) shouldBe Symbol("left") // 1 != 2

      out1.merge(in1) shouldBe Symbol("left") // 1 == 1
      out1.merge(in2).value shouldBe outIn12

      out1.merge(outIn12).value shouldBe outIn12
      out1.merge(outIn21) shouldBe Symbol("left") // 1 != 2
    }

    "be mergeable (TransferInGlobalOffset)" in {
      in1.merge(out1) shouldBe Symbol("left")
      in1.merge(out2).value shouldBe outIn21

      in1.merge(in1).value shouldBe in1
      in1.merge(in2) shouldBe Symbol("left")

      in1.merge(outIn12) shouldBe Symbol("left")
      in1.merge(outIn21).value shouldBe outIn21
    }

    "be mergeable (TransferGlobalOffsets)" in {
      outIn12.merge(out1).value shouldBe outIn12
      outIn12.merge(out2) shouldBe Symbol("left")

      outIn12.merge(in1) shouldBe Symbol("left")
      outIn12.merge(in2).value shouldBe outIn12

      outIn12.merge(outIn12).value shouldBe outIn12
      outIn12.merge(outIn21) shouldBe Symbol("left")
    }
  }
}
