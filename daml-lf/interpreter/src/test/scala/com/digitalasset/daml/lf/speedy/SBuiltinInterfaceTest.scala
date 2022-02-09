// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SError.SError
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue.{SValue => _, _}
import com.daml.lf.testing.parser.Implicits._
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class SBuiltinInterfaceTest
    extends AnyFreeSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  import SBuiltinInterfaceTest._

  "Interfaces" - {
    val iouTypeRep = Ref.TypeConName.assertFromString("-pkgId-:Mod:Iou")
    val alice = Ref.Party.assertFromString("alice")
    val bob = Ref.Party.assertFromString("bob")

    val testCases = Table[String, SValue](
      "expression" -> "string-result",
      "interface_template_type_rep @Mod:Iface Mod:aliceOwesBobIface" -> STypeRep(
        TTyCon(iouTypeRep)
      ),
      "signatory_interface @Mod:Iface Mod:aliceOwesBobIface" -> SList(FrontStack(SParty(alice))),
      "observer_interface @Mod:Iface Mod:aliceOwesBobIface" -> SList(FrontStack(SParty(bob))),
    )

    forEvery(testCases) { (exp, res) =>
      s"""eval[$exp] --> "$res"""" in {
        eval(e"$exp") shouldBe Right(res)
      }
    }
  }
}

object SBuiltinInterfaceTest {

  private val pkg =
    p"""
        module Mod {
         
          interface (this : Iface) = {
              precondition True;
          };

          record @serializable Iou = { i: Party, u: Party, name: Text };
          template (this: Iou) = {
            precondition True;
            signatories Cons @Party [Mod:Iou {i} this] (Nil @Party);
            observers Cons @Party [Mod:Iou {u} this] (Nil @Party);
            agreement "Agreement";
            implements Mod:Iface {};
          };

          val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
          val alice : Party = Mod:mkParty "alice";
          val bob : Party = Mod:mkParty "bob";

          val aliceOwesBob : Mod:Iou = Mod:Iou { i = Mod:alice, u = Mod:bob, name = "alice owes bob" };
          val aliceOwesBobIface : Mod:Iface = to_interface @Mod:Iface @Mod:Iou Mod:aliceOwesBob;
        }

    """

  val compiledPackages =
    PureCompiledPackages.assertBuild(Map(defaultParserParameters.defaultPackageId -> pkg))

  private def eval(e: Expr, onLedger: Boolean = true): Either[SError, SValue] =
    evalSExpr(compiledPackages.compiler.unsafeCompile(e), onLedger)

//  private def evalApp(
//      e: Expr,
//      args: Array[SValue],
//      onLedger: Boolean = true,
//  ): Either[SError, SValue] =
//    evalSExpr(SEApp(compiledPackages.compiler.unsafeCompile(e), args.map(SEValue(_))), onLedger)

  private[this] val committers = Set(Ref.Party.assertFromString("Alice"))

  private def evalSExpr(e: SExpr, onLedger: Boolean): Either[SError, SValue] = {
    val machine =
      if (onLedger)
        Speedy.Machine.fromUpdateSExpr(
          compiledPackages,
          transactionSeed = crypto.Hash.hashPrivateKey("SBuiltinTest"),
          updateSE = SEApp(SEMakeClo(Array(), 2, SELocA(0)), Array(e)),
          committers = committers,
        )
      else
        Speedy.Machine.fromPureSExpr(compiledPackages, e)
    SpeedyTestLib.run(machine)
  }

}
