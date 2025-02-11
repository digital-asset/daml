// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.{LanguageMajorVersion}
import com.digitalasset.daml.lf.speedy.SError.SError
import com.digitalasset.daml.lf.speedy.SExpr.{SEApp, SExpr}
import com.digitalasset.daml.lf.speedy.SValue.SContractId
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.TransactionVersion.VDev
import com.digitalasset.daml.lf.transaction.{GlobalKeyWithMaintainers, Node, Versioned}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import com.daml.logging.LoggingContext
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import java.util

class UpgradeTestV2 extends UpgradeTest(LanguageMajorVersion.V2)

class UpgradeTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  implicit val pkgId: Ref.PackageId = Ref.PackageId.assertFromString("-no-pkg-")

  import SpeedyTestLib.UpgradeVerificationRequest

  private[this] implicit def parserParameters(implicit
      pkgId: Ref.PackageId
  ): ParserParameters[this.type] =
    ParserParameters(pkgId, languageVersion = majorLanguageVersion.dev)

  val ifacePkgId = Ref.PackageId.assertFromString("-iface-")
  private lazy val ifacePkg = {
    implicit def pkgId: Ref.PackageId = ifacePkgId
    p""" metadata ( '-iface-' : '1.0.0' )
    module M {

      record @serializable MyUnit = {};
      interface (this : Iface) = {
        viewtype M:MyUnit;
        method myChoice : Text;

        choice @nonConsuming MyChoice (self) (ctl: Party): Text
          , controllers (Cons @Party [ctl] Nil @Party)
          , observers (Nil @Party)
          to upure @Text (call_method @M:Iface myChoice this);
      };
    }
    """
  }

  val pkgId1 = Ref.PackageId.assertFromString("-pkg1-")
  private lazy val pkg1 = {
    implicit def pkgId: Ref.PackageId = pkgId1
    p""" metadata ( '-upgrade-test-' : '1.0.0' )
    module M {

      record @serializable T = { sig: Party, obs: Party, aNumber: Int64 };
      template (this: T) = {
        precondition True;
        signatories M:mkList (M:T {sig} this) (None @Party);
        observers M:mkList (M:T {obs} this) (None @Party);
        implements '-iface-':M:Iface {
          view = '-iface-':M:MyUnit {};
          method myChoice = "myChoice v1";
        };
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val mkParty : Text -> Party =
        \(t:Text) ->
          case TEXT_TO_PARTY t of
            None -> ERROR @Party "none"
          | Some x -> x;

      val do_create: Text -> Text -> Int64 -> Update (ContractId M:T) =
        \(sig: Text) -> \(obs: Text) -> \(n: Int64) ->
          create @M:T M:T { sig = M:mkParty sig, obs = M:mkParty obs, aNumber = n };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;

      val mkList: Party -> Option Party -> List Party =
        \(sig: Party) -> \(optSig: Option Party) ->
          case optSig of
            None -> Cons @Party [sig] Nil @Party
          | Some extraSig -> Cons @Party [sig, extraSig] Nil @Party;

    }
    """
  }

  val pkgId2: Ref.PackageId = Ref.PackageId.assertFromString("-pkg2-")

  private lazy val pkg2 = {
    // adds a choice to T
    implicit def pkgId: Ref.PackageId = pkgId2
    p""" metadata ( '-upgrade-test-' : '2.0.0' )
      module M {

      record @serializable T = { sig: Party, obs: Party, aNumber: Int64 };
      template (this: T) = {
        precondition True;
        signatories '-pkg1-':M:mkList (M:T {sig} this) (None @Party);
        observers '-pkg1-':M:mkList (M:T {obs} this) (None @Party);
        choice NoOp (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {sig} this] Nil @Party,
          observers Nil @Party
          to upure @Unit ();
        implements '-iface-':M:Iface {
          view = '-iface-':M:MyUnit {};
          method myChoice = "myChoice v2";
        };
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;

      val do_exercise: ContractId M:T -> Update Unit  =
        \(cId: ContractId M:T) -> exercise @M:T NoOp cId ();
      }
    """
  }

  val pkgId3: Ref.PackageId = Ref.PackageId.assertFromString("-pkg3-")
  private lazy val pkg3 = {
    // add an optional additional signatory
    implicit def pkgId: Ref.PackageId = pkgId3
    p""" metadata ( '-upgrade-test-' : '3.0.0' )
      module M {

      record @serializable T = { sig: Party, obs: Party, aNumber: Int64, optSig: Option Party };
      template (this: T) = {
        precondition True;
        signatories '-pkg1-':M:mkList (M:T {sig} this) (M:T {optSig} this);
        observers '-pkg1-':M:mkList (M:T {obs} this) (None @Party);
        choice NoOp (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {sig} this] Nil @Party,
          observers Nil @Party
          to upure @Unit ();
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;

      val do_exercise: ContractId M:T -> Update Unit  =
        \(cId: ContractId M:T) -> exercise @M:T NoOp cId ();
      }
    """
  }

  val pkgId4: Ref.PackageId = Ref.PackageId.assertFromString("-pkg4-")
  private lazy val pkg4 = {
    implicit def pkgId: Ref.PackageId = pkgId4
    p""" metadata ( '-upgrade-test-' : '4.0.0' )
      module M {

      record @serializable T = { sig: Party, obs: Party, aNumber: Int64, optSig: Option Party };
      template (this: T) = {
        precondition True;
        signatories '-pkg1-':M:mkList (M:T {sig} this) (None @Party);
        observers '-pkg1-':M:mkList (M:T {obs} this) (None @Party);
        choice NoOp (self) (u: Unit) : Unit,
          controllers '-pkg1-':M:mkList (M:T {sig} this) (None @Party),
          observers Nil @Party
          to upure @Unit ();
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;

      val do_exercise: ContractId M:T -> Update Unit  =
        \(cId: ContractId M:T) -> exercise @M:T NoOp cId ();
      }
    """
  }

  val pkgName = {
    assert(
      pkg1.pkgName == pkg2.pkgName && pkg2.pkgName == pkg3.pkgName && pkg3.pkgName == pkg4.pkgName
    )
    pkg1.pkgName
  }

  val pkg1Ver = pkg1.pkgVersion
  val pkg2Ver = pkg2.pkgVersion
  val pkg3Ver = pkg3.pkgVersion
  val unknownPkgVer = None

  private lazy val pkgs =
    PureCompiledPackages.assertBuild(
      Map(
        ifacePkgId -> ifacePkg,
        pkgId1 -> pkg1,
        pkgId2 -> pkg2,
        pkgId3 -> pkg3,
        pkgId4 -> pkg4,
      ),
      Compiler.Config.Dev(majorLanguageVersion),
    )

  private val alice = Ref.Party.assertFromString("alice")
  private val bob = Ref.Party.assertFromString("bob")

  val theCid = ContractId.V1(crypto.Hash.hashPrivateKey(s"theCid"))

  type Success = (Value, List[UpgradeVerificationRequest])

  def go(
      e: Expr,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
  ): Either[SError, Success] = {

    val sexprToEval: SExpr = pkgs.compiler.unsafeCompile(e)

    implicit def logContext: LoggingContext = LoggingContext.ForTesting
    val seed = crypto.Hash.hashPrivateKey("seed")
    val machine = Speedy.Machine.fromUpdateSExpr(
      pkgs,
      seed,
      sexprToEval,
      Set(alice, bob),
      packageResolution = packageResolution,
    )

    SpeedyTestLib
      .runCollectRequests(machine)
      .map { case (sv, uvs) => // ignoring any AuthRequest
        val v = sv.toNormalizedValue(VDev)
        (v, uvs)
      }
  }

  // The given contractValue is wrapped as a contract available for ledger-fetch
  def go(e: Expr, contract: ContractInstance): Either[SError, Success] = {

    val se: SExpr = pkgs.compiler.unsafeCompile(e)
    val args: Array[SValue] = Array(SContractId(theCid))
    val sexprToEval: SExpr = SEApp(se, args)

    implicit def logContext: LoggingContext = LoggingContext.ForTesting
    val seed = crypto.Hash.hashPrivateKey("seed")
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, seed, sexprToEval, Set(alice, bob))

    SpeedyTestLib
      .runCollectRequests(machine, getContract = Map(theCid -> Versioned(VDev, contract)))
      .map { case (sv, uvs) => // ignoring any AuthRequest
        val v = sv.toNormalizedValue(VDev)
        (v, uvs)
      }
  }

  // The given contractSValue is wrapped as a disclosedContract
  def goDisclosed(
      e: Expr,
      templateId: Ref.TypeConName,
      contractSValue: SValue,
  ): Either[SError, Success] = {

    val se: SExpr = pkgs.compiler.unsafeCompile(e)
    val args = Array[SValue](SContractId(theCid))
    val sexprToEval = SEApp(se, args)

    implicit def logContext: LoggingContext = LoggingContext.ForTesting
    val seed = crypto.Hash.hashPrivateKey("seed")
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, seed, sexprToEval, Set(alice, bob))

    val contractInfo: Speedy.ContractInfo =
      // NICK: where does this contract-info even get used?
      Speedy.ContractInfo(
        version = VDev,
        packageName = pkgName,
        packageVersion = unknownPkgVer,
        templateId = templateId,
        value = contractSValue,
        signatories = Set.empty,
        observers = Set.empty,
        keyOpt = None,
      )
    machine.addDisclosedContracts(theCid, contractInfo)

    SpeedyTestLib
      .runCollectRequests(machine)
      .map { case (sv, uvs) => // ignoring any AuthRequest
        val v = sv.toNormalizedValue(VDev)
        (v, uvs)
      }
  }

  def makeRecord(fields: Value*): Value = {
    ValueRecord(
      None,
      fields.map { v => (None, v) }.to(ImmArray),
    )
  }

  val v1_base =
    makeRecord(
      ValueParty(alice),
      ValueParty(bob),
      ValueInt64(100),
    )

  val v1_key =
    GlobalKeyWithMaintainers.assertBuild(i"'-pkg1-':M:T", ValueParty(alice), Set(alice), pkgName)

  val v2_key =
    GlobalKeyWithMaintainers.assertBuild(i"'-pkg2-':M:T", ValueParty(alice), Set(alice), pkgName)

  "upgrade attempted" - {

    "missing optional field -- None is manufactured" in {

      val v_missingField =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
        )

      val v_extendedWithNone =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
          ValueOptional(None),
        )

      inside(
        go(
          e"'-pkg3-':M:do_fetch",
          ContractInstance(pkgName, pkg2Ver, i"'-pkg2-':M:T", v_missingField),
        )
      ) { case Right((v, _)) =>
        v shouldBe v_extendedWithNone
      }
    }

    "missing non-optional field -- should be rejected" in {
      // should be caught by package upgradability check
      val v_missingField = makeRecord(ValueParty(alice))

      inside(
        go(
          e"'-pkg1-':M:do_fetch",
          ContractInstance(pkgName, unknownPkgVer, i"'-pkg1-':M:T", v_missingField),
        )
      ) { case Left(SError.SErrorCrash(_, reason)) =>
        reason should include(
          "Unexpected non-optional extra template field type encountered during upgrading"
        )
      }
    }

    "mismatching qualified name -- should be rejected" in {
      val v =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
          ValueOptional(None),
        )

      val negativeTestCase = i"'-pkg2-':M:T"
      val positiveTestCases = Table("tyCon", i"'-pkg2-':M1:T", i"'-pkg2-':M2:T")
      go(
        e"'-pkg3-':M:do_fetch",
        ContractInstance(pkgName, pkg2Ver, negativeTestCase, v),
      ) shouldBe a[
        Right[_, _]
      ]

      forEvery(positiveTestCases) { tyCon =>
        inside(go(e"'-pkg3-':M:do_fetch", ContractInstance(pkgName, unknownPkgVer, tyCon, v))) {
          case Left(e) =>
            // TODO(https://github.com/DACH-NY/canton/issues/23879): do better than a crash once we typecheck values
            //    on import.
            e shouldBe a[SError.SErrorCrash]
        }
      }
    }
  }

  "downgrade attempted" - {

    "correct fields" in {

      val res =
        go(e"'-pkg1-':M:do_fetch", ContractInstance(pkgName, pkg2Ver, i"'-pkg2-':M:T", v1_base))

      inside(res) { case Right((v, _)) =>
        v shouldBe v1_base
      }
    }

    "extra field (text) - something is very wrong" in {
      // should be caught by package upgradability check

      val v1_extraText =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
          ValueText("extra"),
        )

      val res =
        go(
          e"'-pkg1-':M:do_fetch",
          ContractInstance(pkgName, unknownPkgVer, i"'-pkg1-':M:T", v1_extraText),
        )

      inside(res) { case Left(SError.SErrorCrash(_, reason)) =>
        reason should include(
          "Unexpected non-optional extra contract field encountered during downgrading"
        )
      }
    }

    "extra field (Some) - cannot be dropped" in {

      val v1_extraSome =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
          ValueOptional(Some(ValueParty(bob))),
        )

      val res = go(
        e"'-pkg2-':M:do_fetch",
        ContractInstance(pkgName, pkg3Ver, i"'-pkg3-':M:T", v1_extraSome),
      )

      inside(res) { case Left(SError.SErrorDamlException(IE.Dev(_, IE.Dev.Upgrade(e)))) =>
        e shouldBe IE.Dev.Upgrade.DowngradeDropDefinedField(t"'-pkg2-':M:T", v1_extraSome)
      }
    }

    "extra field (None) - OK, downgrade allowed" in {

      val v1_extraNone =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
          Value.ValueOptional(None),
        )

      val res =
        go(
          e"'-pkg1-':M:do_fetch",
          ContractInstance(pkgName, unknownPkgVer, i"'-pkg3-':M:T", v1_extraNone),
        )

      inside(res) { case Right((v, _)) =>
        v shouldBe v1_base
      }
    }
  }

  "upgrade" - {
    "be able to fetch a same contract using different versions" in {
      // The following code is not properly typed, but emulates two commands that fetch a same contract using different versions.
      val res = go(
        e"""\(cid: ContractId '-pkg1-':M:T) ->
               ubind
                 x1: Unit <- '-pkg2-':M:do_fetch cid;
                 x2: Unit <- '-pkg3-':M:do_fetch cid
               in upure @Unit ()
          """,
        ContractInstance(pkgName, pkg1Ver, i"'-pkg1-':M:T", v1_base),
      )
      res shouldBe a[Right[_, _]]
    }

    "be able to fetch a locally created contract using different versions" in {
      val res = go(
        e"""ubind
              cid: ContractId '-pkg1-':M:T <- '-pkg1-':M:do_create "alice" "bob" 100;
              _: '-pkg2-':M:T <- fetch_template @'-pkg2-':M:T cid
            in upure @(ContractId '-pkg1-':M:T) cid
          """
      )
      inside(res) { case Right((v, List())) =>
        v shouldBe a[ValueContractId]
      }
    }

    "be able to fetch by key a locally created contract using different versions" in {
      val res = go(
        e"""let alice : Party = '-pkg1-':M:mkParty "alice"
            in ubind
              cid: ContractId '-pkg1-':M:T <- '-pkg1-':M:do_create "alice" "bob" 100;
              _: '-pkg2-':M:T <- fetch_by_key @'-pkg2-':M:T alice
            in upure @(ContractId '-pkg1-':M:T) cid
          """
      )
      inside(res) { case Right((v, List())) =>
        v shouldBe a[ValueContractId]
      }
    }

    "be able to exercise a locally created contract using different versions" in {
      val res = go(
        e"""ubind
              cid: ContractId '-pkg1-':M:T <- '-pkg1-':M:do_create "alice" "bob" 100;
              _: Unit <- exercise @'-pkg2-':M:T NoOp cid ()
            in upure @(ContractId '-pkg1-':M:T) cid
          """
      )
      inside(res) { case Right((v, List())) =>
        v shouldBe a[ValueContractId]
      }
    }

    "be able to exercise by key a locally created contract using different versions" in {
      val res = go(
        e"""let alice : Party = '-pkg1-':M:mkParty "alice"
            in ubind
                 cid: ContractId '-pkg1-':M:T <- '-pkg1-':M:do_create "alice" "bob" 100;
                 _: Unit <- exercise_by_key @'-pkg2-':M:T NoOp alice ()
               in upure @(ContractId '-pkg1-':M:T) cid
          """
      )
      inside(res) { case Right((v, List())) =>
        v shouldBe a[ValueContractId]
      }
    }

    "be able to exercise by interface locally created contract using different versions" in {
      val res = go(
        e"""let alice : Party = '-pkg1-':M:mkParty "alice"
            in ubind
                 cid: ContractId '-pkg1-':M:T <- '-pkg1-':M:do_create "alice" "bob" 100;
                 res: Text <- exercise_interface @'-iface-':M:Iface
                                MyChoice
                                (COERCE_CONTRACT_ID @'-pkg1-':M:T @'-iface-':M:Iface cid)
                                alice
               in upure @(ContractId '-pkg1-':M:T) cid
          """,
        packageResolution = Map(Ref.PackageName.assertFromString("-upgrade-test-") -> pkgId2),
      )
      inside(res) { case Right((v, List())) =>
        v shouldBe a[ValueContractId]
      }
    }

    "do recompute and check immutability of meta data when using different versions" in {
      // The following code is not properly typed, but emulates two commands that fetch a same contract using different versions.
      val res: Either[SError, (Value, List[UpgradeVerificationRequest])] = go(
        e"""\(cid: ContractId '-pkg1-':M:T) ->
               ubind
                 x1: Unit <- '-pkg2-':M:do_fetch cid;
                 x2: Unit <- '-pkg3-':M:do_fetch cid
               in upure @Unit ()
          """,
        ContractInstance(pkgName, pkg1Ver, i"'-pkg1-':M:T", v1_base),
      )
      res shouldBe a[Right[_, _]]
    }
  }

  "Disclosed contracts" - {

    implicit val pkgId: Ref.PackageId = Ref.PackageId.assertFromString("-no-pkg-")

    "correct fields" in {

      // This is the SValue equivalent of v1_base
      val sv1_base: SValue = {
        def fields = ImmArray(
          n"sig",
          n"obs",
          n"aNumber",
        )
        def values: util.ArrayList[SValue] = ArrayList(
          SValue.SParty(alice), // And it needs to be a party
          SValue.SParty(bob),
          SValue.SInt64(100),
        )
        SValue.SRecord(i"'-pkg1-':M:T", fields, values)
      }
      inside(goDisclosed(e"'-pkg1-':M:do_fetch", i"'-pkg1-':M:T", sv1_base)) { case Right((v, _)) =>
        v shouldBe v1_base
      }
    }

    "requires downgrade" in {

      val sv1_base: SValue = {
        def fields = ImmArray(
          n"sig",
          n"obs",
          n"aNumber",
          n"extraField",
        )
        def values: util.ArrayList[SValue] = ArrayList(
          SValue.SParty(alice),
          SValue.SParty(bob),
          SValue.SInt64(100),
          SValue.SOptional(None),
        )
        SValue.SRecord(i"'-unknown-':M:T", fields, values)
      }
      inside(goDisclosed(e"'-pkg1-':M:do_fetch", i"'-pkg1-':M:T", sv1_base)) { case Right((v, _)) =>
        v shouldBe v1_base
      }
    }
  }

  "Exercise Node" - {

    "is populated with contract ID " in {
      val v_alice_none =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
          ValueOptional(None),
        )

      val se: SExpr = pkgs.compiler.unsafeCompile(e"'-pkg4-':M:do_exercise")
      val args: Array[SValue] = Array(SContractId(theCid))
      val sexprToEval: SExpr = SEApp(se, args)

      implicit def logContext: LoggingContext = LoggingContext.ForTesting

      val seed = crypto.Hash.hashPrivateKey("seed")
      val machine = Speedy.Machine.fromUpdateSExpr(pkgs, seed, sexprToEval, Set(alice, bob))

      val res =
        SpeedyTestLib.buildTransactionCollectRequests(
          machine,
          getContract =
            Map(theCid -> Versioned(VDev, ContractInstance(pkgName, pkg3Ver, i"'-pkg3-':M:T", v_alice_none))),
        )

      inside(res.map(_._1.nodes.values.toList)) { case Right(List(exe: Node.Exercise)) =>
        exe.packageName shouldBe "-upgrade-test-"
        exe.creationPackageId shouldBe Some("-pkg3-")
        exe.templateId.packageId shouldBe "-pkg4-"
      }
    }
  }
}
