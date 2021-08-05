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
      "type synonym application" in {
        val ty = Ast.TSynApp(
          Ref.Identifier.assertFromString(
            "e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b:Module:Foo"
          ),
          ImmArray(
            Ast.TBuiltin(Ast.BTInt64),
            Ast.TBuiltin(Ast.BTText),
          ),
        )
        encodeType(ty).render(80) shouldBe "Module.Foo Int Text"
      }
    }
    //"" in {
    //  def foo(x: Ast.Type): Unit = x match {
    //    case Ast.TSynApp(tysyn, args) =>
    //    case Ast.TApp(tyfun, arg) =>
    //    case Ast.TForall(binder, body) =>
    //    case Ast.TStruct(fields) =>
    //  }
    //}
  }
}
