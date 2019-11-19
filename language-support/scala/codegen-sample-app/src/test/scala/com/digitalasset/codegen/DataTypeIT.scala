// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import com.digitalasset.ledger.client.binding.{Value, Primitive => P}
import com.digitalasset.sample._
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

    "idempotent on enums" in {
      import EnumMod.Color._

      for (color <- List(EnumMod.Color.Red, EnumMod.Color.Blue, EnumMod.Color.Green))
        Value.decode(Value.encode(color)) shouldBe Some(color)

    }

    "idempotent on genMap" in {
      import RecordMod.Pair
      import RecordMod.Pair._
      import VariantMod.Either._
      import VariantMod.Either

      type T = P.GenMap[Pair[P.Int64, P.Numeric], Either[P.Int64, P.Numeric]]

      val genMap: T =
        Map(
          Pair(1L, BigDecimal("1.000")) -> Left(1L),
          Pair(2L, BigDecimal("-2.222")) -> Right(BigDecimal("-2.222")),
          Pair(3L, BigDecimal("3.333")) -> Left(3L)
        )

      Value.decode[T](Value.encode(genMap)) shouldBe Some(genMap)
    }
  }

}
