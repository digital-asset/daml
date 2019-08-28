// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import com.digitalasset.ledger.client.binding.{Value, Primitive => P}
import com.digitalasset.sample.{Enum, Record, Variant}
import org.scalatest.{Matchers, WordSpec}

class DataTypeIT extends WordSpec with Matchers {

  "Value.decode follows by Value.encode" should {

    "idempotent on records" in {
      import Record.Pair._

      type T = Record.Pair[P.Int64, P.Numeric]

      val record: T = Record.Pair(1, 1.1)

      Value.decode[T](Value.encode(record)) shouldBe Some(record)
    }

    "idempotent on variants" in {
      import Variant.Either._

      type T = Variant.Either[P.Int64, P.Numeric]

      val variant1: T = Variant.Either.Left(1)
      val variant2: T = Variant.Either.Right(1.1)

      Value.decode[T](Value.encode(variant1)) shouldBe Some(variant1)
      Value.decode[T](Value.encode(variant2)) shouldBe Some(variant2)
    }

    "enums" in {
      import Enum.Color._

      for (color <- List(Enum.Color.Red, Enum.Color.Blue, Enum.Color.Green))
        Value.decode(Value.encode(color)) shouldBe Some(color)

    }

  }

}
