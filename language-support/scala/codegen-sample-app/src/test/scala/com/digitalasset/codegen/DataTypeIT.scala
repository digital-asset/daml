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

    "idempotent on textMap" in {
      type T = P.TextMap[Long]

      val genMap: T =
        P.TextMap("1" -> 1L, "2" -> 2L, "3" -> 3L)

      Value.decode[T](Value.encode(genMap)) shouldBe Some(genMap)
    }

    val pair1 = RecordMod.Pair(1L, BigDecimal("1.000"))
    val pair2 = RecordMod.Pair(2L, BigDecimal("-2.222"))
    val pair3 = RecordMod.Pair(3L, BigDecimal("3.333"))

    type T = P.GenMap[RecordMod.Pair[P.Int64, P.Numeric], VariantMod.Either[P.Int64, P.Numeric]]

    val genMap: T =
      P.GenMap(
        pair1 -> VariantMod.Either.Left(1L),
        pair2 -> VariantMod.Either.Right(BigDecimal("-2.222")),
        pair3 -> VariantMod.Either.Left(3L)
      )

    "idempotent on genMap" in {
      Value.decode[T](Value.encode(genMap)) shouldBe Some(genMap)
    }

    "preserve order of genMap entries" in {
      Value.decode[T](Value.encode(genMap)).map(_.keys) shouldBe Some(Seq(pair1, pair2, pair3))
    }
  }

}
