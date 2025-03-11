// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SError.{SError, SErrorDamlException}
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SValue.{SValue => _, _}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.{
  GlobalKeyWithMaintainers,
  TransactionVersion,
  Versioned,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractInstance
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import com.digitalasset.daml.lf.interpretation.{Error => IE}

import scala.util.{Failure, Success, Try}

class SBuiltinInterfaceTestDefaultLf
    extends SBuiltinInterfaceTest(
      LanguageVersion.default,
      Compiler.Config.Default(LanguageMajorVersion.V2),
    )
class SBuiltinInterfaceTestDevLf
    extends SBuiltinInterfaceTest(
      LanguageVersion.v2_dev,
      Compiler.Config.Dev(LanguageMajorVersion.V2),
    )

class SBuiltinInterfaceUpgradeImplementationTest extends AnyFreeSpec with Matchers with Inside {

  import EvalHelpers._

  // TODO: revert to the default version and compiler config once they support upgrades
  val languageVersion = LanguageVersion.Features.packageUpgrades
  val compilerConfig = Compiler.Config.Dev(LanguageMajorVersion.V2)

  val alice = Ref.Party.assertFromString("Alice")

  // The following code defines a package -iface-pkg- that defines a single interface Iface.
  val ifacePkgName = Ref.PackageName.assertFromString("-iface-pkg-")
  val ifacePkgId = Ref.PackageId.assertFromString("-iface-pkg-id-")
  val ifaceParserParams = ParserParameters(
    defaultPackageId = ifacePkgId,
    languageVersion = languageVersion,
  )
  val ifacePkg =
    p"""metadata ( '$ifacePkgName' : '1.0.0' )
            module Mod {
              val mkParty : Text -> Party = \(t:Text) ->
                case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;

              record @serializable MyViewTypeA = { n : Int64 };
              interface (this : IfaceA) = {
                viewtype Mod:MyViewTypeA;
                choice @nonConsuming MyChoiceA (self) (u: Unit): Unit
                  , controllers (Cons @Party [Mod:mkParty "Alice"] Nil @Party)
                  , observers (Nil @Party)
                  to upure @Unit ();
              };

              record @serializable MyViewTypeB = { n : Int64 };
              interface (this : IfaceB) = {
                viewtype Mod:MyViewTypeB;
                choice @nonConsuming MyChoiceB (self) (u: Unit): Unit
                  , controllers (Cons @Party [Mod:mkParty "Alice"] Nil @Party)
                  , observers (Nil @Party)
                  to upure @Unit ();
              };
            }
          """ (ifaceParserParams)

  // The following code defines a family of packages -implem-pkg- versions 1.0.0, 2.0.0, ... that define a
  // template T that implements Iface. The view function for version 1 of the package returns 1, the view function
  // of version 2 of the package returns 2, etc.
  val implemPkgName = Ref.PackageName.assertFromString("-implem-pkg-")
  def implemPkgVersion(pkgVersion: Int) =
    Ref.PackageVersion.assertFromString(s"${pkgVersion}.0.0")
  def implemPkgId(pkgVersion: Int) =
    Ref.PackageId.assertFromString(s"-implem-pkg-id-$pkgVersion-")
  def implemParserParams(pkgVersion: Int) = ParserParameters(
    defaultPackageId = implemPkgId(pkgVersion),
    languageVersion = languageVersion,
  )

  def implV1 =
    p"""metadata ( '$implemPkgName' : '${implemPkgVersion(1)}' )
            module Mod {
              record @serializable T = { p: Party };
              template (this: T) = {
                precondition True;
                signatories Cons @Party [Mod:T {p} this] (Nil @Party);
                observers Nil @Party;
                implements '$ifacePkgId':Mod:IfaceA { view = '$ifacePkgId':Mod:MyViewTypeA { n = 1 }; };
              };
            }
          """ (implemParserParams(1))

  def implV2 =
    p"""metadata ( '$implemPkgName' : '${implemPkgVersion(2)}' )
            module Mod {
              record @serializable T = { p: Party };
              template (this: T) = {
                precondition True;
                signatories Cons @Party [Mod:T {p} this] (Nil @Party);
                observers Nil @Party;
                implements '$ifacePkgId':Mod:IfaceA { view = '$ifacePkgId':Mod:MyViewTypeA { n = 2 }; };
                implements '$ifacePkgId':Mod:IfaceB { view = '$ifacePkgId':Mod:MyViewTypeB { n = 2 }; };
              };
            }
          """ (implemParserParams(2))

  // All three of -iface-package-id-, -implem-pkg-id-1-, and -implem-pkg-id-2- are made available to the interpreter.
  val compiledPackages = PureCompiledPackages.assertBuild(
    Map(
      ifacePkgId -> ifacePkg,
      implemPkgId(1) -> implV1,
      implemPkgId(2) -> implV2,
    ),
    compilerConfig,
  )

  def exerciseNewInstance(
      contractVersion: Int,
      preferredVersion: Int,
  ): Try[Either[SError, SValue]] = {
    // We prefer -implem-pkg-$preferredVersion
    val packagePreferences = Map(
      ifacePkgName -> ifacePkgId,
      implemPkgName -> implemPkgId(preferredVersion),
    )

    // We assume one contract of type -implem-pkg-id-$contractVersion-:Mod:T on the ledger
    val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test"))
    val Ast.TTyCon(tplId) = t"Mod:T" (implemParserParams(contractVersion))
    val tplPayload = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice)))
    val contracts = Map[Value.ContractId, Value.VersionedContractInstance](
      cid -> Versioned(
        TransactionVersion.StableVersions.max,
        ContractInstance(implemPkgName, Some(implemPkgVersion(contractVersion)), tplId, tplPayload),
      )
    )

    evalApp(
      e"\(cid: ContractId Mod:IfaceB) -> exercise_interface @Mod:IfaceB MyChoiceB cid ()" (
        ifaceParserParams
      ),
      Array(SContractId(cid), SToken),
      packageResolution = packagePreferences,
      getContract = contracts,
      getPkg = PartialFunction.empty,
      compiledPackages = compiledPackages,
      committers = Set(alice),
    )
  }

  "exercise_interface" - {
    "when package preference points to old package without the new instance, new-versioned contracts miss the instance" in {
      inside(
        exerciseNewInstance(2, 1)
      ) {
        case Success(
              Left(SErrorDamlException(IE.ContractDoesNotImplementInterface(iface, _, tid)))
            ) =>
          iface shouldBe Ref.TypeConName.assertFromString(s"${ifacePkgId}:Mod:IfaceB")
          tid shouldBe Ref.TypeConName.assertFromString(s"${implemPkgId(1)}:Mod:T")
      }
    }

    "when package preference points to package with new instance, new-versioned contracts get the instance" in {
      inside(
        exerciseNewInstance(2, 2)
      ) { case Success(result) =>
        result shouldBe a[Right[_, _]]
      }
    }

    "when package preference points to package with new instance, old-versioned contracts get the instance" in {
      inside(
        exerciseNewInstance(1, 2)
      ) { case Success(result) =>
        result shouldBe a[Right[_, _]]
      }
    }

    "when package preference points to old package without the new instance, old-versioned contracts miss the instance" in {
      inside(
        exerciseNewInstance(1, 1)
      ) {
        case Success(
              Left(SErrorDamlException(IE.ContractDoesNotImplementInterface(iface, _, tid)))
            ) =>
          iface shouldBe Ref.TypeConName.assertFromString(s"${ifacePkgId}:Mod:IfaceB")
          tid shouldBe Ref.TypeConName.assertFromString(s"${implemPkgId(1)}:Mod:T")
      }
    }
  }
}

class SBuiltinInterfaceUpgradeViewTest extends AnyFreeSpec with Matchers with Inside {

  import EvalHelpers._
  import org.scalatest.Inspectors.forEvery

  // TODO: revert to the default version and compiler config once they support upgrades
  val languageVersion = LanguageVersion.Features.packageUpgrades
  val compilerConfig = Compiler.Config.Dev(LanguageMajorVersion.V2)

  val alice = Ref.Party.assertFromString("Alice")

  // The following code defines a package -iface-pkg- that defines a single interface Iface.
  val ifacePkgName = Ref.PackageName.assertFromString("-iface-pkg-")
  val ifacePkgId = Ref.PackageId.assertFromString("-iface-pkg-id-")
  val ifaceParserParams = ParserParameters(
    defaultPackageId = ifacePkgId,
    languageVersion = languageVersion,
  )
  val ifacePkg =
    p"""metadata ( '$ifacePkgName' : '1.0.0' )
            module Mod {
              val mkParty : Text -> Party = \(t:Text) ->
                case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;

              record @serializable MyViewType = { n : Int64 };
              interface (this : Iface) = {
                viewtype Mod:MyViewType;
                choice @nonConsuming MyChoice (self) (u: Unit): Unit
                  , controllers (Cons @Party [Mod:mkParty "Alice"] Nil @Party)
                  , observers (Nil @Party)
                  to upure @Unit ();
              };
            }
          """ (ifaceParserParams)

  // The following code defines a family of packages -implem-pkg- versions 1.0.0, 2.0.0, ... that define a
  // template T that implements Iface. The view function for version 1 of the package returns 1, the view function
  // of version 2 of the package returns 2, etc.
  val implemPkgName = Ref.PackageName.assertFromString("-implem-pkg-")
  def implemPkgVersion(pkgVersion: Int) =
    Ref.PackageVersion.assertFromString(s"${pkgVersion}.0.0")
  def implemPkgId(pkgVersion: Int) =
    Ref.PackageId.assertFromString(s"-implem-pkg-id-$pkgVersion-")
  def implemParserParams(pkgVersion: Int) = ParserParameters(
    defaultPackageId = implemPkgId(pkgVersion),
    languageVersion = languageVersion,
  )
  def implemPkg(pkgVersion: Int) =
    p"""metadata ( '$implemPkgName' : '${implemPkgVersion(pkgVersion)}' )
            module Mod {
              record @serializable T = { p: Party };
              template (this: T) = {
                precondition True;
                signatories Cons @Party [Mod:T {p} this] (Nil @Party);
                observers Nil @Party;
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
    compilerConfig,
  )

  // But we prefer version 2 of -implem-pkg-, which will force an upgrade of any version 1 contract when
  // fetched/exercised by interface
  def packagePreference(pkgVersion: Int) = Map(
    ifacePkgName -> ifacePkgId,
    implemPkgName -> implemPkgId(pkgVersion),
  )

  // We assume one contract of type -implem-pkg-id-1-:Mod:T on the ledger, with ID cid.
  val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test"))
  val Ast.TTyCon(tplV1Id) = t"Mod:T" (implemParserParams(1))
  val tplV1Payload = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice)))
  val contracts = Map[Value.ContractId, Value.VersionedContractInstance](
    cid -> Versioned(
      TransactionVersion.StableVersions.max,
      ContractInstance(implemPkgName, Some(implemPkgVersion(1)), tplV1Id, tplV1Payload),
    )
  )

  "fetch_interface" - {
    "should not reject inconsistent view upgrades" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Iface) -> fetch_interface @Mod:Iface cid" (ifaceParserParams),
          Array(SContractId(cid), SToken),
          packageResolution = packagePreference(2),
          getContract = contracts,
          getPkg = PartialFunction.empty,
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) { case Success(result) =>
        result shouldBe a[Right[_, _]]
      }
    }
  }

  "exercise_interface" - {
    "should not reject inconsistent view upgrades" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Iface) -> exercise_interface @Mod:Iface MyChoice cid ()" (
            ifaceParserParams
          ),
          Array(SContractId(cid), SToken),
          packageResolution = packagePreference(2),
          getContract = contracts,
          getPkg = PartialFunction.empty,
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) { case Success(result) =>
        result shouldBe a[Right[_, _]]
      }
    }
  }

  val ifaceViewTypeId = Ref.Identifier.assertFromString(s"$ifacePkgId:Mod:MyViewType")
  "view_interface" - {
    forEvery(List(1, 2))(preferredVersion =>
      s"should get view implementation v$preferredVersion when package preference is for version $preferredVersion" in {
        inside(
          evalApp(
            e"""\(cid: ContractId Mod:Iface) ->
                  ubind iface:Mod:Iface <- fetch_interface @Mod:Iface cid
                  in upure @Mod:MyViewType view_interface @Mod:Iface iface""" (
              ifaceParserParams
            ),
            Array(SContractId(cid), SToken),
            packageResolution = packagePreference(preferredVersion),
            getContract = contracts,
            getPkg = PartialFunction.empty,
            compiledPackages = compiledPackages,
            committers = Set(alice),
          )
        ) { case Success(Right(SRecord(`ifaceViewTypeId`, _, ArrayList(SInt64(`preferredVersion`))))) =>
          succeed
        }
      }
    )
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
      Ref.TypeConName.assertFromString(s"${basePkgId}:Mod:Iou")
    implicit val parserParameters: ParserParameters[helpers.type] = basePkgParserParams

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
        eval(e"$exp", compiledBasePkgs, Set(alice)) shouldBe Success(Right(res))
      }
    }

    "exercise_interface" - {
      "should prevent view errors from being caught" in {
        val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test"))
        val Ast.TTyCon(tplId) = t"ViewErrorTest:T"
        val tplPayload = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice)))

        inside(
          evalApp(
            e"\(cid: ContractId I0:I0) -> ViewErrorTest:exercise_interface_and_catch_error cid",
            Array(SContractId(cid), SToken),
            packageResolution = pkgNameMap,
            getContract = Map(
              cid -> Versioned(
                TransactionVersion.StableVersions.max,
                ContractInstance(basePkg.pkgName, basePkg.pkgVersion, tplId, tplPayload),
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
                ContractInstance(basePkg.pkgName, basePkg.pkgVersion, tplId, tplPayload),
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
                ContractInstance(
                  packageName = basePkg.pkgName,
                  packageVersion = basePkg.pkgVersion,
                  template = iouId,
                  arg = iouPayload,
                ),
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
                ContractInstance(extraPkg.pkgName, extraPkg.pkgVersion, extraIouId, iouPayload),
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
                ContractInstance(extraPkg.pkgName, extraPkg.pkgVersion, extraIouId, iouPayload),
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

  val basePkgId = Ref.PackageId.assertFromString("-base-package-id-")
  val basePkgParserParams: ParserParameters[this.type] =
    ParserParameters(defaultPackageId = basePkgId, languageVersion = languageVersion)

  lazy val basePkg =
    p""" metadata ( '-base-package-' : '1.0.0' )
        module I0 {
          interface (this: I0) = {
            viewtype Mod:MyUnit;
            choice @nonConsuming MyChoice (self) (u: Unit): Unit
              , controllers (Nil @Party)
              , observers (Nil @Party)
              to upure @Unit ();
            coimplements T_Co0_No1:T_Co0_No1 { view = Mod:MyUnit {}; };
            coimplements T_Co0_Co1:T_Co0_Co1 { view = Mod:MyUnit {}; };
          };
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

          val exercise_interface_and_catch_error : ContractId I0:I0 -> Update Unit =
            \(cid: ContractId I0:I0) ->
              try @Unit
                exercise_interface @I0:I0 MyChoice cid ()
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
            implements Mod:Iface { view = Mod:MyUnit {}; };
          };

          val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
          val alice : Party = Mod:mkParty "Alice";
          val bob : Party = Mod:mkParty "Bob";

          val aliceOwesBob : Mod:Iou = Mod:Iou { i = Mod:alice, u = Mod:bob, name = "alice owes bob" };
          val aliceOwesBobIface : Mod:Iface = to_interface @Mod:Iface @Mod:Iou Mod:aliceOwesBob;
        }
    """ (basePkgParserParams)

  val basePkgs = Map(basePkgId -> basePkg)

  lazy val compiledBasePkgs = PureCompiledPackages.assertBuild(basePkgs, compilerConfig)

  val Ast.TTyCon(iouId) = t"'$basePkgId':Mod:Iou" (basePkgParserParams)

  val extraPkgId = Ref.PackageId.assertFromString("-extra-package-id-")
  val extraPkgParserParams = basePkgParserParams.copy(defaultPackageId = extraPkgId)
  require(extraPkgId != basePkgId)

  val extraPkg = p""" metadata ( '-extra-package-name-' : '1.0.0' )
        module Mod {

          record @serializable Iou = { i: Party, u: Party, name: Text };

          template (this: Iou) = {
            precondition True;
            signatories Cons @Party [Mod:Iou {i} this] (Nil @Party);
            observers Cons @Party [Mod:Iou {u} this] (Nil @Party);
            implements '$basePkgId':Mod:Iface { view = '$basePkgId':Mod:MyUnit {} ; };
          };

          val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
          val alice : Party = Mod:mkParty "Alice";
          val bob : Party = Mod:mkParty "Bob";

        }
    """ (extraPkgParserParams)

  lazy val compiledExtendedPkgs =
    PureCompiledPackages.assertBuild(basePkgs + (extraPkgId -> extraPkg), compilerConfig)

  val pkgNameMap = Map(
    basePkg.pkgName -> basePkgId,
    extraPkg.pkgName -> extraPkgId,
  )

  val Ast.TTyCon(extraIouId) = t"Mod:Iou" (extraPkgParserParams)

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
