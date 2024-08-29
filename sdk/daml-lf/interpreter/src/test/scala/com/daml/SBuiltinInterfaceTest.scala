// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.daml.lf.speedy.SError.{SError, SErrorDamlException}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue.{SValue => _, _}
import com.daml.lf.testing.parser
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.{GlobalKeyWithMaintainers, TransactionVersion, Versioned}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractInstance
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import com.daml.lf.interpretation.{Error => IE}
import scala.util.{Failure, Success, Try}

class SBuiltinInterfaceTestDefaultLf
    extends SBuiltinInterfaceTest(
      LanguageVersion.default,
      Compiler.Config.Default(LanguageMajorVersion.V1),
    )
class SBuiltinInterfaceTestEarlyAccessLf
    extends SBuiltinInterfaceTest(
      LanguageVersion.Features.packageUpgrades,
      Compiler.Config.Dev(LanguageMajorVersion.V1),
    )

class SBuiltinInterfaceUpgradeTest extends AnyFreeSpec with Matchers with Inside {

  import EvalHelpers._

  val alice = Ref.Party.assertFromString("Alice")

  // The following code defines a package -iface-pkg- that defines a single interface Iface.
  val ifacePkgName = Ref.PackageName.assertFromString("-iface-pkg-")
  val ifacePkgId = Ref.PackageId.assertFromString("-iface-package-id-")
  val ifaceParserParams = ParserParameters(
    defaultPackageId = ifacePkgId,
    // TODO: revert to the default version once it supports upgrades
    languageVersion = LanguageVersion.Features.packageUpgrades,
  )
  val ifacePkg =
    p"""metadata ( '$ifacePkgName' : '1.0.0' )
            module Mod {
              record @serializable MyViewType = { n : Int64 };
              interface (this : Iface) = {
                viewtype Mod:MyViewType;
                choice @nonConsuming MyChoice (self) (u: Unit): Unit
                  , controllers (Nil @Party)
                  , observers (Nil @Party)
                  to upure @Unit ();
              };
            }
          """ (ifaceParserParams)

  // The following code defines a family of packages -implem-pkg- versions 1.0.0, 2.0.0, ... that define a
  // template T that implements Iface. The view function for version 1 of the package returns 1, the view function
  // of version 2 of the package returns 2, etc.
  val implemPkgName = Ref.PackageName.assertFromString("-implem-pkg-")
  def implemPkgId(pkgVersion: Int) =
    Ref.PackageId.assertFromString(s"-implem-pkg-id-$pkgVersion-")
  def implemParserParams(pkgVersion: Int) = ParserParameters(
    defaultPackageId = implemPkgId(pkgVersion),
    // TODO: revert to the default version once it supports upgrades
    languageVersion = LanguageVersion.Features.packageUpgrades,
  )
  def implemPkg(pkgVersion: Int) =
    p"""metadata ( '$implemPkgName' : '$pkgVersion.0.0' )
            module Mod {
              record @serializable T = { p: Party };
              template (this: T) = {
                precondition True;
                signatories Cons @Party [Mod:T {p} this] (Nil @Party);
                observers Nil @Party;
                agreement "Agreement";
                implements '$ifacePkgId':Mod:Iface { view = '$ifacePkgId':Mod:MyViewType { n = $pkgVersion }; };
              };
            }
          """ (implemParserParams(pkgVersion))

  // All three of -iface-package-id-, -implem-pkg-id-1-, and -implem-pkg-id-2- are made available to the interpreter.
  val compiledPackages = PureCompiledPackages.assertBuild(
    Map(
      ifacePkgId -> ifacePkg,
      implemPkgId(1) -> implemPkg(1),
      implemPkgId(2) -> implemPkg(2),
    ),
    // TODO: revert to the default compiler config once it supports upgrades
    Compiler.Config.Dev(LanguageMajorVersion.V1),
  )

  // But we prefer version 2 of -implem-pkg-, which will force an upgrade of the contract and trigger a view
  // consistency check, which is expected to fail.
  val packagePreferences = Map(
    ifacePkgName -> ifacePkgId,
    implemPkgName -> implemPkgId(2),
  )

  // We assume one contract of type -implem-pkg-id-1-:Mod:T on the ledger, with ID cid.
  val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test"))
  val Ast.TTyCon(tplV1Id) = t"Mod:T" (implemParserParams(1))
  val tplV1Payload = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice)))
  val contracts = Map[Value.ContractId, Value.VersionedContractInstance](
    cid -> Versioned(
      TransactionVersion.StableVersions.max,
      ContractInstance(Some(implemPkgName), tplV1Id, tplV1Payload),
    )
  )

  "fetch_interface" - {
    "should reject inconsistent view upgrades" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Iface) -> fetch_interface @Mod:Iface cid" (ifaceParserParams),
          Array(SContractId(cid), SToken),
          packageResolution = packagePreferences,
          getContract = contracts,
          getPkg = PartialFunction.empty,
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) { case Success(Left(SErrorDamlException(IE.Dev(_, IE.Dev.Upgrade(upgradeError))))) =>
        upgradeError shouldBe a[IE.Dev.Upgrade.ViewMismatch]
      }
    }
  }

  "exercise_interface" - {
    "should reject inconsistent view upgrades" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Iface) -> exercise_interface @Mod:Iface MyChoice cid ()" (
            ifaceParserParams
          ),
          Array(SContractId(cid), SToken),
          packageResolution = packagePreferences,
          getContract = contracts,
          getPkg = PartialFunction.empty,
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) { case Success(Left(SErrorDamlException(IE.Dev(_, IE.Dev.Upgrade(upgradeError))))) =>
        upgradeError shouldBe a[IE.Dev.Upgrade.ViewMismatch]
      }
    }
  }
}

class SBuiltinInterfaceTest(languageVersion: LanguageVersion, compilerConfig: Compiler.Config)
    extends AnyFreeSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  import EvalHelpers._
  val helpers = new SBuiltinInterfaceTestHelpers(languageVersion, compilerConfig)
  import helpers._

  "Interface operations" - {
    val iouTypeRep =
      Ref.TypeConName.assertFromString(s"${parserParameters.defaultPackageId}:Mod:Iou")

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
        eval(e"$exp", compiledBasePkgs, Set(alice)) shouldBe Success(Right(res))
      }
    }

    "fetch_interface" - {
      "should prevent view errors from being caught" in {
        val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test"))
        val Ast.TTyCon(tplId) = t"ViewErrorTest:T"
        val tplPayload = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice)))

        inside(
          evalApp(
            e"\(cid: ContractId I0:I0) -> ViewErrorTest:fetch_interface_and_catch_error cid",
            Array(SContractId(cid), SToken),
            packageResolution = pkgNameMap,
            getContract = Map(
              cid -> Versioned(
                TransactionVersion.StableVersions.max,
                ContractInstance(Some(basePkgName), tplId, tplPayload),
              )
            ),
            getPkg = PartialFunction.empty,
            compiledPackages = compiledBasePkgs,
            committers = Set(alice),
          )
        ) { case Success(Left(error)) =>
          // We expect the error throw by the view to not have been caught by
          // fetch_interface_and_catch_error.
          error shouldBe a[SErrorDamlException]
        }
      }

      "should request unknown package before everything else" in {

        val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test"))

        inside(
          evalApp(
            e"\(cid: ContractId Mod:Iface) -> fetch_interface @Mod:Iface cid",
            Array(SContractId(cid), SToken),
            packageResolution = pkgNameMap,
            getContract = Map(
              cid -> Versioned(
                TransactionVersion.StableVersions.max,
                ContractInstance(basePkg.name, iouId, iouPayload),
              )
            ),
            getPkg = PartialFunction.empty,
            compiledPackages = compiledBasePkgs,
            committers = Set(alice),
          )
        ) { case Success(result) =>
          result shouldBe a[Right[_, _]]
        }

        inside(
          evalApp(
            e"\(cid: ContractId Mod:Iface) -> fetch_interface @Mod:Iface cid",
            Array(SContractId(cid), SToken),
            packageResolution = pkgNameMap,
            getContract = Map(
              cid -> Versioned(
                TransactionVersion.StableVersions.max,
                ContractInstance(Some(extraPkgName), extraIouId, iouPayload),
              )
            ),
            getPkg = PartialFunction.empty,
            compiledPackages = compiledBasePkgs,
            committers = Set(alice),
          )
        ) { case Failure(err) =>
          err shouldBe a[SpeedyTestLib.UnknownPackage]
        }

        inside(
          evalApp(
            e"\(cid: ContractId Mod:Iface) -> fetch_interface @Mod:Iface cid",
            Array(SContractId(cid), SToken),
            packageResolution = pkgNameMap,
            getContract = Map(
              cid -> Versioned(
                TransactionVersion.StableVersions.max,
                ContractInstance(Some(extraPkgName), extraIouId, iouPayload),
              )
            ),
            getPkg = { case `extraPkgId` =>
              compiledExtendedPkgs
            },
            compiledPackages = compiledBasePkgs,
            committers = Set(alice),
          )
        ) { case Success(result) =>
          result shouldBe a[Right[_, _]]
        }
      }

    }
  }
}

final class SBuiltinInterfaceTestHelpers(
    val languageVersion: LanguageVersion,
    val compilerConfig: Compiler.Config,
) {

  val alice = Ref.Party.assertFromString("Alice")
  val bob = Ref.Party.assertFromString("Bob")

  val basePkgName = Ref.PackageName.assertFromString("-base-package-")
  val basePkgId = Ref.PackageId.assertFromString("-base-package-id-")
  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters(defaultPackageId = basePkgId, languageVersion = languageVersion)

  lazy val basePkg =
    p""" metadata ( '$basePkgName' : '1.0.0' )
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
          val alice : Party = Mod:mkParty "Alice";

          val callGetText : I0:I0 -> Text = \(x: I0:I0) ->
            case from_required_interface @I0:I0 @I1:I1 x of
              None -> "does not (co)implement I1" | Some x -> call_method @I1:I1 getText x;

          val t_Co0_No1 : I0:I0 = to_interface @I0:I0 @T_Co0_No1:T_Co0_No1 (T_Co0_No1:T_Co0_No1 { party = MethodTest:alice, msg = "T_Co0_No1" });
          val t_Co0_Co1 : I0:I0 = to_interface @I0:I0 @T_Co0_Co1:T_Co0_Co1 (T_Co0_Co1:T_Co0_Co1 { party = MethodTest:alice, msg = "T_Co0_Co1" });
          val t_Im0_No1 : I0:I0 = to_interface @I0:I0 @T_Im0_No1:T_Im0_No1 (T_Im0_No1:T_Im0_No1 { party = MethodTest:alice, msg = "T_Im0_No1" });
          val t_Im0_Co1 : I0:I0 = to_interface @I0:I0 @T_Im0_Co1:T_Im0_Co1 (T_Im0_Co1:T_Im0_Co1 { party = MethodTest:alice, msg = "T_Im0_Co1" });
          val t_Im0_Im1 : I0:I0 = to_interface @I0:I0 @T_Im0_Im1:T_Im0_Im1 (T_Im0_Im1:T_Im0_Im1 { party = MethodTest:alice, msg = "T_Im0_Im1" });
        }

        module ViewErrorTest {
          val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
          val alice : Party = ViewErrorTest:mkParty "Alice";
          record @serializable T = { party: Party };

          record @serializable Ex = { message: Text } ;
          exception Ex = {
            message \(e: ViewErrorTest:Ex) -> ViewErrorTest:Ex {message} e
          };

          template (this: T) = {
            precondition True;
            signatories Cons @Party [ViewErrorTest:T {party} this] (Nil @Party);
            observers (Nil @Party);
            agreement "";
            implements I0:I0 { view = throw @Mod:MyUnit @ViewErrorTest:Ex (ViewErrorTest:Ex { message = "user error" }); };
          };

          val fetch_interface_and_catch_error : ContractId I0:I0 -> Update Unit =
            \(cid: ContractId I0:I0) ->
              try @Unit
                ubind _:I0:I0 <- fetch_interface @I0:I0 cid
                in upure @Unit ()
              catch
                e -> Some @(Update Unit) (upure @Unit ())
          ;
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
          val alice : Party = Mod:mkParty "Alice";
          val bob : Party = Mod:mkParty "Bob";

          val aliceOwesBob : Mod:Iou = Mod:Iou { i = Mod:alice, u = Mod:bob, name = "alice owes bob" };
          val aliceOwesBobIface : Mod:Iface = to_interface @Mod:Iface @Mod:Iou Mod:aliceOwesBob;
        }
    """

  val basePkgs = Map(basePkgId -> basePkg)

  lazy val compiledBasePkgs = PureCompiledPackages.assertBuild(basePkgs, compilerConfig)

  val Ast.TTyCon(iouId) = t"'$basePkgId':Mod:Iou"

  val extraPkgName = Ref.PackageName.assertFromString("-extra-package-name-")
  val extraPkgId = Ref.PackageId.assertFromString("-extra-package-id-")
  require(extraPkgId != basePkgId)

  lazy val extendedPkgs = {
    val modifiedParserParameters: parser.ParserParameters[this.type] =
      parserParameters.copy(defaultPackageId = extraPkgId)

    val pkg = p""" metadata ( '$extraPkgName' : '1.0.0' )
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
          val alice : Party = Mod:mkParty "Alice";
          val bob : Party = Mod:mkParty "Bob";

        }
    """ (modifiedParserParameters)
    basePkgs + (modifiedParserParameters.defaultPackageId -> pkg)
  }
  lazy val compiledExtendedPkgs = PureCompiledPackages.assertBuild(extendedPkgs, compilerConfig)

  val pkgNameMap = Map(
    basePkgName -> basePkgId,
    extraPkgName -> extraPkgId,
  )

  val Ast.TTyCon(extraIouId) = t"'$extraPkgId':Mod:Iou"

  val iouPayload =
    Value.ValueRecord(
      None,
      ImmArray(
        None -> Value.ValueParty(alice),
        None -> Value.ValueParty(bob),
        None -> Value.ValueText("name"),
      ),
    )
}

object EvalHelpers {
  import SpeedyTestLib.loggingContext

  def eval(
      e: Expr,
      compiledPackages: PureCompiledPackages,
      committers: Set[Ref.Party],
  ): Try[Either[SError, SValue]] =
    evalSExpr(
      compiledPackages.compiler.unsafeCompile(e),
      Map.empty,
      PartialFunction.empty,
      PartialFunction.empty,
      PartialFunction.empty,
      compiledPackages,
      committers,
    )

  def evalApp(
      e: Expr,
      args: Array[SValue],
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      getPkg: PartialFunction[Ref.PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
      compiledPackages: PureCompiledPackages,
      committers: Set[Ref.Party],
  ): Try[Either[SError, SValue]] =
    evalSExpr(
      SEApp(compiledPackages.compiler.unsafeCompile(e), args),
      packageResolution,
      getPkg,
      getContract,
      getKey,
      compiledPackages,
      committers,
    )

  def evalSExpr(
      e: SExpr,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      getPkg: PartialFunction[Ref.PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance],
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId],
      compiledPackages: PureCompiledPackages,
      committers: Set[Ref.Party],
  ): Try[Either[SError, SValue]] = {
    val machine =
      Speedy.Machine.fromUpdateSExpr(
        compiledPackages,
        packageResolution = packageResolution,
        transactionSeed = crypto.Hash.hashPrivateKey("SBuiltinTest"),
        updateSE = SELet1(e, SEMakeClo(Array(SELocS(1)), 1, SELocF(0))),
        committers = committers,
      )
    Try(SpeedyTestLib.run(machine, getPkg, getContract, getKey))
  }
}
