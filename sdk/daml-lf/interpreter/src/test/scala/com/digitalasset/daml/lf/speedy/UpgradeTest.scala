// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.interpretation.{Error => IE}
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.daml.lf.speedy.SError.SError
import com.daml.lf.speedy.SExpr.{SEApp, SExpr}
import com.daml.lf.speedy.SValue.SContractId
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.TransactionVersion.V16
import com.daml.lf.transaction.{GlobalKeyWithMaintainers, Versioned}
import com.daml.lf.value.Value
import com.daml.lf.value.Value._
import com.daml.logging.LoggingContext
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import java.util

class UpgradeTestV1 extends UpgradeTest(LanguageMajorVersion.V1)
//class UpgradeTestV2 extends UpgradeTest(LanguageMajorVersion.V2)

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
    ParserParameters(pkgId, languageVersion = majorLanguageVersion.maxStableVersion)

  lazy val pkgId0 = Ref.PackageId.assertFromString("-pkg0-")
  private lazy val pkg0 = {
    implicit def parserParameters: ParserParameters[this.type] =
      ParserParameters(defaultPackageId = pkgId0, languageVersion = LanguageVersion.v1_15)
    p""" metadata ( '-upgrade-test-' : '1.0.0' )
    module M {

      record @serializable T = { sig: Party, obs: Party, aNumber: Int64 };
      template (this: T) = {
        precondition True;
        signatories M:mkList (M:T {sig} this) (None @Party);
        observers M:mkList (M:T {obs} this) (None @Party);
        agreement "Agreement";
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

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
        agreement "Agreement";
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

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
    // same signatures as pkg1
    implicit def pkgId: Ref.PackageId = pkgId2
    p""" metadata ( '-upgrade-test-' : '2.0.0' )
      module M {

      record @serializable T = { sig: Party, obs: Party, aNumber: Int64 };
      template (this: T) = {
        precondition True;
        signatories '-pkg1-':M:mkList (M:T {sig} this) (None @Party);
        observers '-pkg1-':M:mkList (M:T {obs} this) (None @Party);
        agreement "Agreement";
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;
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
        agreement "Agreement";
        key @Party (M:T {sig} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;

      }
    """
  }

  val pkgId4: Ref.PackageId = Ref.PackageId.assertFromString("-pkg4-")
  private lazy val pkg4 = {
    // swap signatories and observers with respect to pkg2
    implicit def pkgId: Ref.PackageId = pkgId4
    p""" metadata ( '-upgrade-test-' : '4.0.0' )
      module M {

      record @serializable T = { sig: Party, obs: Party, aNumber: Int64, optSig: Option Party };
      template (this: T) = {
        precondition True;
        signatories '-pkg1-':M:mkList (M:T {obs} this) (None @Party);
        observers '-pkg1-':M:mkList (M:T {sig} this) (None @Party);
        agreement "Agreement";
        key @Party (M:T {obs} this) (\ (p: Party) -> Cons @Party [p] Nil @Party);
      };

      val do_fetch: ContractId M:T -> Update M:T =
        \(cId: ContractId M:T) ->
          fetch_template @M:T cId;
      }
    """
  }

  val pkgName = {
    assert(
      pkg0.metadata.map(
        _.name
      ) == pkg1.name && pkg1.name == pkg2.name && pkg2.name == pkg3.name && pkg3.name == pkg4.name
    )
    pkg1.name
  }

  private lazy val pkgs =
    PureCompiledPackages.assertBuild(
      Map(pkgId0 -> pkg0, pkgId1 -> pkg1, pkgId2 -> pkg2, pkgId3 -> pkg3, pkgId4 -> pkg4),
      Compiler.Config.Dev(majorLanguageVersion),
    )

  private val alice = Ref.Party.assertFromString("alice")
  private val bob = Ref.Party.assertFromString("bob")

  val theCid = ContractId.V1(crypto.Hash.hashPrivateKey(s"theCid"))

  type Success = (Value, List[UpgradeVerificationRequest])

  // The given contractValue is wrapped as a contract available for ledger-fetch
  def go(e: Expr, contract: ContractInstance): Either[SError, Success] = {

    val se: SExpr = pkgs.compiler.unsafeCompile(e)
    val args: Array[SValue] = Array(SContractId(theCid))
    val sexprToEval: SExpr = SEApp(se, args)

    implicit def logContext: LoggingContext = LoggingContext.ForTesting
    val seed = crypto.Hash.hashPrivateKey("seed")
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, seed, sexprToEval, Set(alice, bob))

    SpeedyTestLib
      .runCollectRequests(machine, getContract = Map(theCid -> Versioned(V16, contract)))
      .map { case (sv, _, uvs) => // ignoring any AuthRequest
        val v = sv.toNormalizedValue(V16)
        (v, uvs)
      }
  }

  // The given contractSValue is wrapped as a disclosedContract
  def goDisclosed(e: Expr, contractSValue: SValue): Either[SError, Success] = {

    val se: SExpr = pkgs.compiler.unsafeCompile(e)
    val args = Array[SValue](SContractId(theCid))
    val sexprToEval = SEApp(se, args)

    implicit def logContext: LoggingContext = LoggingContext.ForTesting
    val seed = crypto.Hash.hashPrivateKey("seed")
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, seed, sexprToEval, Set(alice, bob))

    val contractInfo: Speedy.ContractInfo =
      // NICK: where does this contract-info even get used?
      Speedy.ContractInfo(
        version = V16,
        packageName = pkgName,
        templateId = i"'-unknown-':M:T",
        value = contractSValue,
        agreementText = "meh",
        signatories = Set.empty,
        observers = Set.empty,
        keyOpt = None,
      )
    machine.addDisclosedContracts(theCid, contractInfo)

    SpeedyTestLib
      .runCollectRequests(machine)
      .map { case (sv, _, uvs) => // ignoring any AuthRequest
        val v = sv.toNormalizedValue(V16)
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
    GlobalKeyWithMaintainers.assertBuild(
      i"'-pkg1-':M:T",
      ValueParty(alice),
      Set(alice),
      crypto.Hash.KeyPackageName.assertBuild(pkgName, V16),
    )

  "upgrade attempted" - {

    "contract is LF < 1.16 -- should be reject" in {
      val v =
        makeRecord(
          ValueParty(alice),
          ValueParty(bob),
          ValueInt64(100),
        )

      go(e"'-pkg1-':M:do_fetch", ContractInstance(pkg0.name, i"'-pkg0-':M:T", v)) shouldBe a[
        Left[_, _]
      ]
    }

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
        go(e"'-pkg3-':M:do_fetch", ContractInstance(pkgName, i"'-pkg2-':M:T", v_missingField))
      ) { case Right((v, _)) =>
        v shouldBe v_extendedWithNone
      }
    }

    "missing non-optional field -- should be rejected" in {
      // should be caught by package upgradability check
      val v_missingField = makeRecord(ValueParty(alice))

      inside(
        go(e"'-pkg1-':M:do_fetch", ContractInstance(pkgName, i"'-unknow-':M:T", v_missingField))
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

      val expectedTyCon = i"'-pkg3-':M:T"
      val negativeTestCase = i"'-pkg2-':M:T"
      val positiveTestCases = Table("tyCon", i"'-pkg2-':M1:T", i"'-pkg2-':M2:T")
      go(e"'-pkg3-':M:do_fetch", ContractInstance(pkgName, negativeTestCase, v)) shouldBe a[
        Right[_, _]
      ]

      forEvery(positiveTestCases) { tyCon =>
        inside(go(e"'-pkg3-':M:do_fetch", ContractInstance(pkgName, tyCon, v))) {
          case Left(SError.SErrorDamlException(e)) =>
            e shouldBe IE.WronglyTypedContract(theCid, expectedTyCon, tyCon)
        }
      }
    }
  }

  "downgrade attempted" - {

    "correct fields" in {

      val res = go(e"'-pkg1-':M:do_fetch", ContractInstance(pkgName, i"'-pkg2-':M:T", v1_base))

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
        go(e"'-pkg1-':M:do_fetch", ContractInstance(pkgName, i"'-unknown-':M:T", v1_extraText))

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

      val res = go(e"'-pkg2-':M:do_fetch", ContractInstance(pkgName, i"'-pkg3-':M:T", v1_extraSome))

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
        go(e"'-pkg2-':M:do_fetch", ContractInstance(pkgName, i"'-unknow-':M:T", v1_extraNone))

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
        ContractInstance(pkgName, i"'-pkg1-':M:T", v1_base),
      )
      res shouldBe a[Right[_, _]]
    }
    "do recompute and check immutability of meta data when using different versions" in {
      // The following code is not properly typed, but emulates two commands that fetch a same contract using different versions.
      val res: Either[SError, (Value, List[UpgradeVerificationRequest])] = go(
        e"""\(cid: ContractId '-pkg1-':M:T) ->
               ubind
                 x1: Unit <- '-pkg2-':M:do_fetch cid;
                 x2: Unit <- '-pkg4-':M:do_fetch cid
               in upure @Unit ()
          """,
        ContractInstance(pkgName, i"'-pkg1-':M:T", v1_base),
      )
      inside(res) { case Right((_, verificationRequests)) =>
        val v4_key = GlobalKeyWithMaintainers.assertBuild(
          i"'-pkg1-':M:T",
          ValueParty(bob),
          Set(bob),
          crypto.Hash.KeyPackageName.assertBuild(pkgName, V16),
        )
        verificationRequests shouldBe List(
          UpgradeVerificationRequest(theCid, Set(alice), Set(bob), Some(v1_key)),
          UpgradeVerificationRequest(theCid, Set(bob), Set(alice), Some(v4_key)),
        )
      }
    }
  }

  "Correct calls to ResultNeedUpgradeVerification" in {

    implicit val pkgId: Ref.PackageId = Ref.PackageId.assertFromString("-no-pkg-")

    val v_alice_none =
      makeRecord(
        ValueParty(alice),
        ValueParty(bob),
        ValueInt64(100),
        ValueOptional(None),
      )

    val v_alice_some =
      makeRecord(
        ValueParty(alice),
        ValueParty(bob),
        ValueInt64(100),
        ValueOptional(Some(ValueParty(bob))),
      )

    inside(go(e"'-pkg3-':M:do_fetch", ContractInstance(pkgName, i"'-pgk3-':M:T", v_alice_none))) {
      case Right((v, List(uv))) =>
        v shouldBe v_alice_none
        uv.coid shouldBe theCid
        uv.signatories.toList shouldBe List(alice)
        uv.observers.toList shouldBe List(bob)
        uv.keyOpt shouldBe Some(v1_key)
    }

    inside(go(e"'-pkg3-':M:do_fetch", ContractInstance(pkgName, i"'-pgk3-':M:T", v_alice_some))) {
      case Right((v, List(uv))) =>
        v shouldBe v_alice_some
        uv.coid shouldBe theCid
        uv.signatories.toList shouldBe List(alice, bob)
        uv.observers.toList shouldBe List(bob)
        uv.keyOpt shouldBe Some(v1_key)
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
      inside(goDisclosed(e"'-pkg1-':M:do_fetch", sv1_base)) { case Right((v, _)) =>
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
      inside(goDisclosed(e"'-pkg1-':M:do_fetch", sv1_base)) { case Right((v, _)) =>
        v shouldBe v1_base
      }
    }

  }

}
