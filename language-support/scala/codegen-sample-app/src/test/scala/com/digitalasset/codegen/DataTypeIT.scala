// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import com.digitalasset.ledger.client.binding.{Value, Primitive => P}
import com.digitalasset.sample.{EnumMod, RecordMod, VariantMod}
import org.scalatest.{Matchers, WordSpec}

class DataTypeIT extends WordSpec with Matchers {

  "Value.decode follows by Value.encode" should {

    "idempotent on records" in {
      import RecordMod.Pair._

      type T = RecordMod.Pair[P.Int64, P.Numeric]

      val record: T = RecordMod.Pair(1, 1.1)

      Value.decode[T](Value.encode(record)) shouldBe Some(record)
    }

    "idempotent on variants" in {
      import VariantMod.Either._

      type T = VariantMod.Either[P.Int64, P.Numeric]

      val variant1: T = VariantMod.Either.Left(1)
      val variant2: T = VariantMod.Either.Right(1.1)

      Value.decode[T](Value.encode(variant1)) shouldBe Some(variant1)
      Value.decode[T](Value.encode(variant2)) shouldBe Some(variant2)
    }

    "enums" in {
      import EnumMod.Color._

      for (color <- List(EnumMod.Color.Red, EnumMod.Color.Blue, EnumMod.Color.Green))
        Value.decode(Value.encode(color)) shouldBe Some(color)

    }

  }

}
