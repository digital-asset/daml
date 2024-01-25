// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data._
import com.daml.lf.language.{Ast, LanguageMajorVersion}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SError.SError
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue.{SValue => _, _}
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.daml.lf.testing.parser
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.{GlobalKeyWithMaintainers, TransactionVersion, Versioned}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractInstance
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import util.{Failure, Success, Try}

class SBuiltinInterfaceTestV2 extends SBuiltinInterfaceTest(LanguageMajorVersion.V2)

class SBuiltinInterfaceTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  val helpers = new SBuiltinInterfaceTestHelpers(majorLanguageVersion)
  import helpers.{parserParameters => _, _}

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)
  val defaultPackageId = parserParameters.defaultPackageId

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
            packageResolution = basePkgNameMap,
            getContract = Map(
              cid -> Versioned(
                TransactionVersion.StableVersions.max,
                ContractInstance(basePkg.name, iouId, iouPayload),
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
            packageResolution = basePkgNameMap,
            getContract = Map(
              cid -> Versioned(
                TransactionVersion.StableVersions.max,
                ContractInstance(extraPkgName, extraIouId, iouPayload),
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
            packageResolution = basePkgNameMap,
            getContract = Map(
              cid -> Versioned(
                TransactionVersion.StableVersions.max,
                ContractInstance(extraPkgName, extraIouId, iouPayload),
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

final class SBuiltinInterfaceTestHelpers(majorLanguageVersion: LanguageMajorVersion) {

  import SpeedyTestLib.loggingContext

  val alice = Ref.Party.assertFromString("Alice")
  val bob = Ref.Party.assertFromString("Bob")

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)
  val basePkgId = parserParameters.defaultPackageId
  val compilerConfig = Compiler.Config.Default(majorLanguageVersion)

  lazy val basePkg =
    p"""  metadata ( 'basic-package' : '1.0.0' )
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
            implements Mod:Iface { view = Mod:MyUnit {}; };
          };

          val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
          val alice : Party = Mod:mkParty "alice";
          val bob : Party = Mod:mkParty "bob";

          val aliceOwesBob : Mod:Iou = Mod:Iou { i = Mod:alice, u = Mod:bob, name = "alice owes bob" };
          val aliceOwesBobIface : Mod:Iface = to_interface @Mod:Iface @Mod:Iou Mod:aliceOwesBob;
        }
    """

  val basePkgs = Map(basePkgId -> basePkg)

  lazy val compiledBasePkgs = PureCompiledPackages.assertBuild(basePkgs, compilerConfig)

  val Ast.TTyCon(iouId) = t"'$basePkgId':Mod:Iou"

  // We assume extraPkg use the same version as basePkg
  val extraPkgName = Ref.PackageName.assertFromString("-extra-package-name-")
  val extraPkgId = Ref.PackageId.assertFromString("-extra-package-id-")
  require(extraPkgId != basePkgId)

  val basePkgNameMap =
    List(basePkg.name, extraPkgName).map(n => n -> basePkgId).toMap

  lazy val extendedPkgs = {

    val modifiedParserParameters: parser.ParserParameters[this.type] =
      parserParameters.copy(defaultPackageId = extraPkgId)

    val pkg = p""" metadata ( 'extended-pkg' : '1.0.0' )
        module Mod {

          record @serializable Iou = { i: Party, u: Party, name: Text };

          template (this: Iou) = {
            precondition True;
            signatories Cons @Party [Mod:Iou {i} this] (Nil @Party);
            observers Cons @Party [Mod:Iou {u} this] (Nil @Party);
            implements '$basePkgId':Mod:Iface { view = '$basePkgId':Mod:MyUnit {} ; };
          };

          val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
          val alice : Party = Mod:mkParty "alice";
          val bob : Party = Mod:mkParty "bob";

        }
    """ (modifiedParserParameters)
    basePkgs + (modifiedParserParameters.defaultPackageId -> pkg)
  }
  lazy val compiledExtendedPkgs = PureCompiledPackages.assertBuild(extendedPkgs, compilerConfig)

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

  def eval(e: Expr): Try[Either[SError, SValue]] =
    evalSExpr(
      compiledBasePkgs.compiler.unsafeCompile(e),
      Map.empty,
      PartialFunction.empty,
      PartialFunction.empty,
      PartialFunction.empty,
    )

  def evalApp(
      e: Expr,
      args: Array[SValue],
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      getPkg: PartialFunction[Ref.PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
  ): Try[Either[SError, SValue]] =
    evalSExpr(
      SEApp(compiledBasePkgs.compiler.unsafeCompile(e), args),
      packageResolution,
      getPkg,
      getContract,
      getKey,
    )

  def evalSExpr(
      e: SExpr,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      getPkg: PartialFunction[Ref.PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance],
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId],
  ): Try[Either[SError, SValue]] = {
    val machine =
      Speedy.Machine.fromUpdateSExpr(
        compiledBasePkgs,
        packageResolution = packageResolution,
        transactionSeed = crypto.Hash.hashPrivateKey("SBuiltinTest"),
        updateSE = SELet1(e, SEMakeClo(Array(SELocS(1)), 1, SELocF(0))),
        committers = Set(alice),
      )
    Try(SpeedyTestLib.run(machine, getPkg, getContract, getKey))
  }

}
