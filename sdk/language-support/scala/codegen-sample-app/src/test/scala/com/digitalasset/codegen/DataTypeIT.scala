// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen

import com.daml.ledger.client.binding.{Value, Primitive => P}
import com.daml.sample._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import shapeless.test.illTyped

class DataTypeIT extends AnyWordSpec with Matchers {

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
        pair3 -> VariantMod.Either.Left(3L),
      )

    "idempotent on genMap" in {
      Value.decode[T](Value.encode(genMap)) shouldBe Some(genMap)
    }
  }

  "variance" should {
    import MyMain.{Maybe, JustMap}
    "default to covariant" in {
      val just = Maybe.Just(42)
      val nothing = Maybe.Nothing[Nothing](())
      Seq[Maybe[Any]](just, nothing) // compiles
    }

    "use invariant where needed" in {
      val m = JustMap(P.GenMap(1L -> 2L))
      val vw: JustMap[P.Int64, Any] = m
      "val kvw: JustMap[Any, Any] = m" shouldNot typeCheck
      val kevw: JustMap[_ <: P.Int64, Any] = m
      Seq[JustMap[_ <: P.Int64, Any]](m, vw, kevw)
    }
  }

  "contract IDs" should {
    @annotation.nowarn("cat=unused&msg=local val sleId in")
    val sleId: P.ContractId[MyMain.SimpleListExample] = P.ContractId("fakesle")
    val imId: P.ContractId[MyMain.InterfaceMixer] = P.ContractId("fakeimid")
    val itmId: P.ContractId[MyMainIface.IfaceFromAnotherMod] = P.ContractId("fakeitmid")

    "coerce from template to interface" in {
      val ifId = imId.toInterface[MyMainIface.IfaceFromAnotherMod]
      (ifId: MyMainIface.IfaceFromAnotherMod.ContractId) should ===(imId)
      illTyped(
        "sleId.toInterface[MyMainIface.IfaceFromAnotherMod]",
        "com.daml.sample.MyMain.SimpleListExample is not a template that implements interface com.daml.sample.MyMainIface.IfaceFromAnotherMod",
      )
      illTyped(
        "itmId.toInterface[MyMainIface.IfaceFromAnotherMod]",
        "value toInterface is not a member of .*ContractId.*IfaceFromAnotherMod.*",
      )
    }

    "coerce from interface to template" in {
      val tpId = itmId.unsafeToTemplate[MyMain.InterfaceMixer]
      (tpId: MyMain.InterfaceMixer.ContractId) should ===(itmId)
      illTyped(
        "itmId.unsafeToTemplate[MyMain.SimpleListExample]",
        ".*SimpleListExample is not a template that implements interface .*IfaceFromAnotherMod",
      )
      illTyped(
        "itmId.unsafeToTemplate[MyMainIface.IfaceFromAnotherMod]",
        ".*IfaceFromAnotherMod is not a template that implements interface .*IfaceFromAnotherMod",
      )
      illTyped(
        "imId.unsafeToTemplate[MyMain.InterfaceMixer]",
        "value unsafeToTemplate is not a member of .*ContractId.*InterfaceMixer.*",
      )
    }
  }
}
