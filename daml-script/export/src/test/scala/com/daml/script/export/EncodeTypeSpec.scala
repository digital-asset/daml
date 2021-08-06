// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.Ast
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeTypeSpec extends AnyFreeSpec with Matchers {
  import Encode._
  import AstSyntax._

  "encodeType" - {
    "builtin types" - {
      val builtins: Seq[(Ast.BuiltinType, String)] = Seq(
        Ast.BTInt64 -> "Int",
        Ast.BTNumeric -> "Numeric",
        Ast.BTText -> "Text",
        Ast.BTTimestamp -> "Time",
        Ast.BTParty -> "Party",
        Ast.BTUnit -> "()",
        Ast.BTBool -> "Bool",
        Ast.BTList -> "[]",
        Ast.BTOptional -> "Optional",
        Ast.BTTextMap -> "DA.TextMap.TextMap",
        Ast.BTGenMap -> "DA.Map.Map",
        Ast.BTUpdate -> "Update",
        Ast.BTScenario -> "Scenario",
        Ast.BTDate -> "Date",
        Ast.BTContractId -> "ContractId",
        Ast.BTArrow -> "(->)",
        Ast.BTAny -> "Any",
        Ast.BTTypeRep -> "TypeRep",
        Ast.BTAnyException -> "AnyException",
        Ast.BTRoundingMode -> "RoundingMode",
        Ast.BTBigNumeric -> "BigNumeric",
      )
      builtins foreach { case (ty, repr) =>
        s"$ty" in {
          encodeType(Ast.TBuiltin(ty)).render(80) shouldBe repr
        }
      }
    }
    "simple types" - {
      "type variable" in {
        encodeType(vFoo).render(80) shouldBe "foo"
      }
      "natural number" in {
        encodeType(Ast.TNat.values(2)).render(80) shouldBe "2"
      }
      "type constructor" in {
        encodeType(Ast.TTyCon(nFoo)).render(80) shouldBe "Module.Foo"
      }
    }
    "composed types" - {
      "type synonym application" in {
        val ty = synApp(nFoo, int, synApp(nBar, text))
        encodeType(ty).render(80) shouldBe "Module.Foo Int (Module.Bar Text)"
      }
      "type application" in {
        val ty = vFoo :@ int :@ (vBar :@ text)
        encodeType(ty).render(80) shouldBe "foo Int (bar Text)"
      }
      "tuple type" in {
        val ty = tuple(int, text, vFoo :@ int)
        encodeType(ty).render(80) shouldBe "(Int, Text, foo Int)"
      }
      "list type" in {
        val ty = list(int)
        encodeType(ty).render(80) shouldBe "[Int]"
      }
      "arrow type" in {
        val ty = (vFoo :@ int =>: text) =>: int =>: text
        encodeType(ty).render(80) shouldBe "(foo Int -> Text) -> Int -> Text"
      }
    }
  }
}

private object AstSyntax {
  implicit class AstApp(private val lhs: Ast.Type) extends AnyVal {
    def :@(rhs: Ast.Type): Ast.Type = {
      Ast.TApp(lhs, rhs)
    }
  }
  implicit class AstArrow(private val rhs: Ast.Type) extends AnyVal {
    def =>:(lhs: Ast.Type): Ast.Type = {
      val arrowTy: Ast.Type = Ast.TBuiltin(Ast.BTArrow)
      Ast.TApp(Ast.TApp(arrowTy, lhs), rhs)
    }
  }
  def synApp(syn: Ref.TypeSynName, args: Ast.Type*): Ast.Type = {
    Ast.TSynApp(syn, ImmArray(args: _*))
  }
  def tuple(tys: Ast.Type*): Ast.Type = {
    val tupleTyCon: Ast.Type = Ast.TTyCon(
      Ref.TypeConName.assertFromString(
        "40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7:DA.Types:Tuple"
      )
    )
    tys.foldLeft(tupleTyCon) { case (acc, ty) => acc :@ ty }
  }
  def list(ty: Ast.Type): Ast.Type = {
    Ast.TBuiltin(Ast.BTList) :@ ty
  }
  val int: Ast.Type = Ast.TBuiltin(Ast.BTInt64)
  val text: Ast.Type = Ast.TBuiltin(Ast.BTText)
  val vFoo: Ast.Type = Ast.TVar(Ref.Name.assertFromString("foo"))
  val vBar: Ast.Type = Ast.TVar(Ref.Name.assertFromString("bar"))
  val nFoo: Ref.Identifier = Ref.TypeSynName.assertFromString("e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b:Module:Foo")
  val nBar: Ref.Identifier = Ref.TypeSynName.assertFromString("e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b:Module:Bar")
}
