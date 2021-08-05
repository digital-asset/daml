// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

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
  }
}
