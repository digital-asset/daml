// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.Ast
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeTypeSpec extends AnyFreeSpec with Matchers {
  import Encode._

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
        encodeType(Ast.TVar(Ref.Name.assertFromString("foo"))).render(80) shouldBe "foo"
      }
      "natural number" in {
        encodeType(Ast.TNat.values(2)).render(80) shouldBe "2"
      }
      "type constructor" in {
        encodeType(
          Ast.TTyCon(
            Ref.Identifier.assertFromString(
              "e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b:Module:Foo"
            )
          )
        ).render(80) shouldBe "Module.Foo"
      }
    }
    "composed types" - {
      val int: Ast.Type = Ast.TBuiltin(Ast.BTInt64)
      val text: Ast.Type = Ast.TBuiltin(Ast.BTText)
      def tuple(tys: Ast.Type*): Ast.Type = {
        val tupleTyCon: Ast.Type = Ast.TTyCon(
          Ref.TypeConName.assertFromString(
            "40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7:DA.Types:Tuple"
          )
        )
        tys.foldLeft(tupleTyCon) { case (acc, ty) => Ast.TApp(acc, ty) }
      }
      def list(ty: Ast.Type): Ast.Type = {
        Ast.TApp(Ast.TBuiltin(Ast.BTList), ty)
      }
      def arrow(ty: Ast.Type, tys: Ast.Type*): Ast.Type = {
        val arrowTy: Ast.Type = Ast.TBuiltin(Ast.BTArrow)
        val rhs = tys.foldRight(None: Option[Ast.Type => Ast.Type]) {
          case (ty, None) => Some((hole: Ast.Type) => Ast.TApp(Ast.TApp(arrowTy, hole), ty))
          case (ty, Some(acc)) => Some((hole: Ast.Type) => Ast.TApp(Ast.TApp(arrowTy, hole), acc(ty)))
        }
        rhs match {
          case Some(acc) => acc(ty)
          case None => ty
        }
      }
      "type synonym application" in {
        val ty = Ast.TSynApp(
          Ref.Identifier.assertFromString(
            "e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b:Module:Foo"
          ),
          ImmArray(
            int,
            Ast.TSynApp(
              Ref.Identifier.assertFromString(
                "e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b:Module:Bar"
              ),
              ImmArray(text),
            ),
          ),
        )
        encodeType(ty).render(80) shouldBe "Module.Foo Int (Module.Bar Text)"
      }
      "type application" in {
        val ty = Ast.TApp(
          Ast.TApp(
            Ast.TVar(Ref.Name.assertFromString("foo")),
            int,
          ),
          Ast.TApp(
            Ast.TVar(Ref.Name.assertFromString("bar")),
            text,
          ),
        )
        encodeType(ty).render(80) shouldBe "foo Int (bar Text)"
      }
      "tuple type" in {
        val ty = tuple(int, text)
        encodeType(ty).render(80) shouldBe "(Int, Text)"
      }
      "list type" in {
        val ty = list(int)
        encodeType(ty).render(80) shouldBe "[Int]"
      }
      "arrow type" in {
        val ty = arrow(arrow(Ast.TApp(Ast.TVar(Ref.Name.assertFromString("foo")), int), text), int, text)
        encodeType(ty).render(80) shouldBe "(foo Int -> Text) -> Int -> Text"
      }
    }
  }
}
