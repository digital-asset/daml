// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data._
import com.daml.lf.language.Ast
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SError.SError
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue.{SValue => _, _}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser
import com.daml.lf.transaction.{GlobalKeyWithMaintainers, TransactionVersion, Versioned}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractInstance
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import util.{Failure, Success, Try}

class SBuiltinInterfaceTest
    extends AnyFreeSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  import SBuiltinInterfaceTest._

  "Interface operations" - {
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
      "MethodTest:callGetText MethodTest:t_Co0_No1" -> SText("does not (co)implement I1"),
      "MethodTest:callGetText MethodTest:t_Co0_Co1" -> SText(
        "co-implements I1 T_Co0_Co1, msg=T_Co0_Co1"
      ),
      "MethodTest:callGetText MethodTest:t_Im0_No1" -> SText("does not (co)implement I1"),
      "MethodTest:callGetText MethodTest:t_Im0_Co1" -> SText(
        "co-implements I1 T_Im0_Co1, msg=T_Im0_Co1"
      ),
      "MethodTest:callGetText MethodTest:t_Im0_Im1" -> SText(
        "implements I1 T_Im0_Im1, msg=T_Im0_Im1"
      ),
    )

    forEvery(testCases) { (exp, res) =>
      s"""eval[$exp] --> "$res"""" in {
        eval(e"$exp") shouldBe Success(Right(res))
      }
    }

    "fetch_interface" - {

      "should request unknown package before everything else" in {

        val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test"))

        inside(
          evalApp(
            e"\(cid: ContractId Mod:Iface) -> fetch_interface @Mod:Iface cid",
            Array(SContractId(cid), SToken),
            true,
            getContract = Map(
              cid -> Versioned(
                TransactionVersion.StableVersions.max,
                ContractInstance(iouId, iouPayload),
              )
            ),
            getPkg = PartialFunction.empty,
          )
        ) { case Success(result) =>
          result shouldBe a[Right[_, _]]
        }

        inside(
          evalApp(
            e"\(cid: ContractId Mod:Iface) -> fetch_interface @Mod:Iface cid",
            Array(SContractId(cid), SToken),
            true,
            getContract = Map(
              cid -> Versioned(
                TransactionVersion.StableVersions.max,
                ContractInstance(extraIouId, iouPayload),
              )
            ),
            getPkg = PartialFunction.empty,
          )
        ) { case Failure(err) =>
          err shouldBe a[SpeedyTestLib.UnknownPackage]
        }

        inside(
          evalApp(
            e"\(cid: ContractId Mod:Iface) -> fetch_interface @Mod:Iface cid",
            Array(SContractId(cid), SToken),
            true,
            getContract = Map(
              cid -> Versioned(
                TransactionVersion.StableVersions.max,
                ContractInstance(extraIouId, iouPayload),
              )
            ),
            getPkg = { case `extraPkgId` =>
              compiledExtendedPkgs
            },
          )
        ) { case Success(result) =>
          result shouldBe a[Right[_, _]]
        }
      }

    }
  }
}

object SBuiltinInterfaceTest {

  import SpeedyTestLib.loggingContext

  private[this] val alice = Ref.Party.assertFromString("Alice")
  private[this] val bob = Ref.Party.assertFromString("Bob")

  import defaultParserParameters.{defaultPackageId => basePkgId}

  private[this] lazy val basePkgs = {
    val pkg =
      p"""
        module T_Co0_No1 {
          record @serializable T_Co0_No1 = { party: Party, msg: Text };

          template (this: T_Co0_No1) = {
            precondition True;
            signatories Cons @Party [T_Co0_No1:T_Co0_No1 {party} this] (Nil @Party);
            observers Cons @Party [T_Co0_No1:T_Co0_No1 {party} this] (Nil @Party);
            agreement "";
          };
        }

        module T_Co0_Co1 {
          record @serializable T_Co0_Co1 = { party: Party, msg: Text };

          template (this: T_Co0_Co1) = {
            precondition True;
            signatories Cons @Party [T_Co0_Co1:T_Co0_Co1 {party} this] (Nil @Party);
            observers Cons @Party [T_Co0_Co1:T_Co0_Co1 {party} this] (Nil @Party);
            agreement "";
          };
        }

        module I0 {
          interface (this: I0) = {
            viewtype Mod:MyUnit;
            coimplements T_Co0_No1:T_Co0_No1 { view = Mod:MyUnit {}; };
            coimplements T_Co0_Co1:T_Co0_Co1 { view = Mod:MyUnit {}; };
          };
        }

        module T_Im0_No1 {
          record @serializable T_Im0_No1 = { party: Party, msg: Text };

          template (this: T_Im0_No1) = {
            precondition True;
            signatories Cons @Party [T_Im0_No1:T_Im0_No1 {party} this] (Nil @Party);
            observers Cons @Party [T_Im0_No1:T_Im0_No1 {party} this] (Nil @Party);
            agreement "";
            implements I0:I0 { view = Mod:MyUnit {}; };
          };
        }

        module T_Im0_Co1 {
          record @serializable T_Im0_Co1 = { party: Party, msg: Text };

          template (this: T_Im0_Co1) = {
            precondition True;
            signatories Cons @Party [T_Im0_Co1:T_Im0_Co1 {party} this] (Nil @Party);
            observers Cons @Party [T_Im0_Co1:T_Im0_Co1 {party} this] (Nil @Party);
            agreement "";
            implements I0:I0 { view = Mod:MyUnit {}; };
          };
        }

        module I1 {
          interface (this: I1) = {
            viewtype Mod:MyUnit;
            requires I0:I0;
            method getText: Text;
            coimplements T_Co0_Co1:T_Co0_Co1 {
              view = Mod:MyUnit {};
              method getText = APPEND_TEXT "co-implements I1 T_Co0_Co1, msg=" (T_Co0_Co1:T_Co0_Co1 {msg} this);
            };
            coimplements T_Im0_Co1:T_Im0_Co1 {
              view = Mod:MyUnit {};
              method getText = APPEND_TEXT "co-implements I1 T_Im0_Co1, msg=" (T_Im0_Co1:T_Im0_Co1 {msg} this);
            };
          };
        }

        module T_Im0_Im1 {
          record @serializable T_Im0_Im1 = { party: Party, msg: Text };

          template (this: T_Im0_Im1) = {
            precondition True;
            signatories Cons @Party [T_Im0_Im1:T_Im0_Im1 {party} this] (Nil @Party);
            observers Cons @Party [T_Im0_Im1:T_Im0_Im1 {party} this] (Nil @Party);
            agreement "";
            implements I0:I0 { view = Mod:MyUnit {}; };
            implements I1:I1 {
              view = Mod:MyUnit {};
              method getText = APPEND_TEXT "implements I1 T_Im0_Im1, msg=" (T_Im0_Im1:T_Im0_Im1 {msg} this);
            };
          };
        }

        module MethodTest {
          val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
          val alice : Party = Mod:mkParty "alice";

          val callGetText : I0:I0 -> Text = \(x: I0:I0) ->
            case from_required_interface @I0:I0 @I1:I1 x of
              None -> "does not (co)implement I1" | Some x -> call_method @I1:I1 getText x;

          val t_Co0_No1 : I0:I0 = to_interface @I0:I0 @T_Co0_No1:T_Co0_No1 (T_Co0_No1:T_Co0_No1 { party = MethodTest:alice, msg = "T_Co0_No1" });
          val t_Co0_Co1 : I0:I0 = to_interface @I0:I0 @T_Co0_Co1:T_Co0_Co1 (T_Co0_Co1:T_Co0_Co1 { party = MethodTest:alice, msg = "T_Co0_Co1" });
          val t_Im0_No1 : I0:I0 = to_interface @I0:I0 @T_Im0_No1:T_Im0_No1 (T_Im0_No1:T_Im0_No1 { party = MethodTest:alice, msg = "T_Im0_No1" });
          val t_Im0_Co1 : I0:I0 = to_interface @I0:I0 @T_Im0_Co1:T_Im0_Co1 (T_Im0_Co1:T_Im0_Co1 { party = MethodTest:alice, msg = "T_Im0_Co1" });
          val t_Im0_Im1 : I0:I0 = to_interface @I0:I0 @T_Im0_Im1:T_Im0_Im1 (T_Im0_Im1:T_Im0_Im1 { party = MethodTest:alice, msg = "T_Im0_Im1" });
        }

        module Mod {

          record @serializable MyUnit = {};

          interface (this : Iface) = {
            viewtype Mod:MyUnit;
          };

          record @serializable Iou = { i: Party, u: Party, name: Text };
          template (this: Iou) = {
            precondition True;
            signatories Cons @Party [Mod:Iou {i} this] (Nil @Party);
            observers Cons @Party [Mod:Iou {u} this] (Nil @Party);
            agreement "Agreement";
            implements Mod:Iface { view = Mod:MyUnit {}; };
          };

          val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
          val alice : Party = Mod:mkParty "alice";
          val bob : Party = Mod:mkParty "bob";

          val aliceOwesBob : Mod:Iou = Mod:Iou { i = Mod:alice, u = Mod:bob, name = "alice owes bob" };
          val aliceOwesBobIface : Mod:Iface = to_interface @Mod:Iface @Mod:Iou Mod:aliceOwesBob;
        }
    """
    Map(defaultParserParameters.defaultPackageId -> pkg)
  }
  lazy val compiledBasePkgs = PureCompiledPackages.assertBuild(basePkgs)

  private[lf] val Ast.TTyCon(iouId) = t"'$basePkgId':Mod:Iou"

  private val extraPkgId = Ref.PackageId.assertFromString("-extra-package-")
  assume(extraPkgId != basePkgId)

  private lazy val extendedPkgs = {

    implicit val defaultParserParameters: parser.ParserParameters[this.type] =
      parser.Implicits.defaultParserParameters.copy(defaultPackageId = extraPkgId)

    val pkg =
      p"""
        module Mod {

          record @serializable MyUnit = {};
          record @serializable Iou = { i: Party, u: Party, name: Text };

          template (this: Iou) = {
            precondition True;
            signatories Cons @Party [Mod:Iou {i} this] (Nil @Party);
            observers Cons @Party [Mod:Iou {u} this] (Nil @Party);
            agreement "Agreement";
            implements '$basePkgId':Mod:Iface { view = '$basePkgId':Mod:MyUnit {} ; };
          };

          val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
          val alice : Party = Mod:mkParty "alice";
          val bob : Party = Mod:mkParty "bob";

        }
    """
    basePkgs + (defaultParserParameters.defaultPackageId -> pkg)
  }
  lazy val compiledExtendedPkgs = PureCompiledPackages.assertBuild(extendedPkgs)

  private val Ast.TTyCon(extraIouId) = t"'$extraPkgId':Mod:Iou"

  private val iouPayload =
    Value.ValueRecord(
      None,
      ImmArray(
        None -> Value.ValueParty(alice),
        None -> Value.ValueParty(bob),
        None -> Value.ValueText("name"),
      ),
    )

  private def eval(e: Expr, onLedger: Boolean = true): Try[Either[SError, SValue]] =
    evalSExpr(
      compiledBasePkgs.compiler.unsafeCompile(e),
      onLedger,
      PartialFunction.empty,
      PartialFunction.empty,
      PartialFunction.empty,
    )

  def evalApp(
      e: Expr,
      args: Array[SValue],
      onLedger: Boolean,
      getPkg: PartialFunction[Ref.PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  ): Try[Either[SError, SValue]] =
    evalSExpr(
      SEApp(compiledBasePkgs.compiler.unsafeCompile(e), args),
      onLedger,
      getPkg,
      getContract,
      getKey,
    )

  def evalSExpr(
      e: SExpr,
      onLedger: Boolean,
      getPkg: PartialFunction[Ref.PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance],
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId],
  ): Try[Either[SError, SValue]] = {
    val machine =
      if (onLedger)
        Speedy.Machine.fromUpdateSExpr(
          compiledBasePkgs,
          transactionSeed = crypto.Hash.hashPrivateKey("SBuiltinTest"),
          updateSE = SELet1(e, SEMakeClo(Array(SELocS(1)), 1, SELocF(0))),
          committers = Set(alice),
        )
      else
        Speedy.Machine.fromPureSExpr(compiledBasePkgs, e)
    Try(SpeedyTestLib.run(machine, getPkg, getContract, getKey))
  }

}
