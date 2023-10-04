// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{PackageId, Party, Identifier}
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.speedy.SError.SError
import com.daml.lf.speedy.SExpr.{SExpr, SEApp}
import com.daml.lf.speedy.SValue.SContractId
import com.daml.lf.testing.parser.Implicits.{defaultParserParameters => _, _}
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.TransactionVersion.VDev
import com.daml.lf.transaction.Versioned
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext

import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class UpgradeTest extends AnyFreeSpec with Matchers with Inside {

  import SpeedyTestLib.UpgradeVerificationRequest

  private def makePP(pid: PackageId): ParserParameters[this.type] = {
    ParserParameters(pid, languageVersion = LanguageVersion.v1_dev)
  }

  private def parseType(pid: PackageId, s: String): Type = {
    implicit val pp: ParserParameters[this.type] = makePP(pid)
    t"$s"
  }

  private def parseExpr(pid: PackageId, s: String): Expr = {
    implicit val pp: ParserParameters[this.type] = makePP(pid)
    e"$s"
  }

  private def parsePackage(pid: PackageId, s: String): Package = {
    implicit val pp: ParserParameters[this.type] = makePP(pid)
    p"$s"
  }

  private def parseTyCon(pid: PackageId, s: String): Identifier = {
    parseType(pid, s) match {
      case TTyCon(tycon) => tycon
      case _ => sys.error("unexpected error")
    }
  }

  private val pid1: PackageId = PackageId.assertFromString("P1")
  private val package1: Package = parsePackage(
    pid1,
    """
    module M1 {

      record @serializable T1 = { theSig: Party, aNumber: Int64, someText: Text};
      template (this: T1) = {
        precondition True;
        signatories Cons @Party [M1:T1 {theSig} this] Nil @Party;
        observers Nil @Party;
        agreement "Agreement";
      };

      val do_fetch: ContractId M1:T1 -> Update M1:T1 =
        \(cId: ContractId M1:T1) ->
          fetch_template @M1:T1 cId;

    }

    module M2 {

      record @serializable T1 = { theSig: Party, aNumber: Int64, optText: Option Text};
      template (this: T1) = {
        precondition True;
        signatories Cons @Party [M2:T1 {theSig} this] Nil @Party;
        observers Nil @Party;
        agreement "Agreement";
      };

      val do_fetch: ContractId M2:T1 -> Update M2:T1 =
        \(cId: ContractId M2:T1) ->
          fetch_template @M2:T1 cId;

    }


    module M3 {

      record @serializable T1 = { one: Party, two: Party };
      template (this: T1) = {
        precondition True;
        signatories Cons @Party [M3:T1 {one} this] Nil @Party;
        observers Nil @Party;
        agreement "Agreement";
      };

      val do_fetch: ContractId M3:T1 -> Update M3:T1 =
        \(cId: ContractId M3:T1) ->
          fetch_template @M3:T1 cId;

    }

    module M4 {

      record @serializable T1 = { one: Party, two: Party };
      template (this: T1) = {
        precondition True;
        signatories Cons @Party [M4:T1 {two} this] Nil @Party; // Like M3 but with diff signatories
        observers Nil @Party;
        agreement "Agreement";
      };

      val do_fetch: ContractId M4:T1 -> Update M4:T1 =
        \(cId: ContractId M4:T1) ->
          fetch_template @M4:T1 cId;

    }


  """,
  )

  private val pkgs = {

    val packageMap = Map(pid1 -> package1)

    PureCompiledPackages.assertBuild(
      packageMap,
      Compiler.Config.Dev.copy(enableContractUpgrading = true),
    )
  }

  private val alice = Party.assertFromString("alice")
  private val bob = Party.assertFromString("bob")

  val theCid = Value.ContractId.V1(crypto.Hash.hashPrivateKey(s"theCid"))

  type Success = (Value, List[UpgradeVerificationRequest])

  def go(entryPoint: String, contractValue: Value): Either[SError, Success] = {

    val e: Expr = parseExpr(pid1, entryPoint)
    val se: SExpr = pkgs.compiler.unsafeCompile(e)
    val args: Array[SValue] = Array(SContractId(theCid))
    val sexprToEval: SExpr = SEApp(se, args)

    implicit def logContext: LoggingContext = LoggingContext.ForTesting
    val seed = crypto.Hash.hashPrivateKey("seed")
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, seed, sexprToEval, Set(alice, bob))

    val contract: Versioned[Value.ContractInstance] = {
      val tycon: Identifier = parseTyCon(pid1, "Mxxx:Txxx") // ignored
      Versioned(VDev, Value.ContractInstance(tycon, contractValue))
    }

    val getContract: Map[Value.ContractId, Value.VersionedContractInstance] =
      Map(theCid -> contract)

    SpeedyTestLib
      .runCollectRequests(machine, getContract = getContract)
      .map { case (sv, _, uvs) => // ignoring any AuthRequest
        val v = sv.toNormalizedValue(VDev)
        (v, uvs)
      }
  }

  def makeRecord(fields: Value*): Value = {
    Value.ValueRecord(
      None,
      fields.map { v => (None, v) }.to(ImmArray),
    )
  }

  "downgrade attempted" - {

    // These tests check downgrade of differently shaped actual contracts
    // Always expecting a contract of type M1:T1

    val v1_base =
      makeRecord(
        Value.ValueParty(alice),
        Value.ValueInt64(100),
        Value.ValueText("lala"),
      )

    "correct fields" in {

      val res = go("M1:do_fetch", v1_base)

      inside(res) { case Right((v, _)) =>
        v shouldBe v1_base
      }
    }

    "extra field (text) - something is very wrong" in {

      val v1_extraText =
        makeRecord(
          Value.ValueParty(alice),
          Value.ValueInt64(100),
          Value.ValueText("lala"),
          Value.ValueText("extra"),
        )

      val res = go("M1:do_fetch", v1_extraText)

      inside(res) { case Left(err) =>
        err.toString should include(
          "Unexpected non-optional extra contract field encountered during downgrading: something is very wrong."
        )
      }

    }

    "extra field (Some) - cannot be dropped" in {

      val v1_extraSome =
        makeRecord(
          Value.ValueParty(alice),
          Value.ValueInt64(100),
          Value.ValueText("lala"),
          Value.ValueOptional(Some(Value.ValueText("heyhey"))),
        )

      val res = go("M1:do_fetch", v1_extraSome)

      inside(res) { case Left(err) =>
        err.toString should include(
          "An optional contract field with a value of Some may not be dropped during downgrading"
        )
      }
    }

    "extra field (None) - OK, downgrade allowed" in {

      val v1_extraNone =
        makeRecord(
          Value.ValueParty(alice),
          Value.ValueInt64(100),
          Value.ValueText("lala"),
          Value.ValueOptional(None),
        )

      val res = go("M1:do_fetch", v1_extraNone)

      inside(res) { case Right((v, _)) =>
        v shouldBe v1_base
      }
    }
  }

  "upgrade attempted" - {

    "missing optional field -- None is manufactured" in {

      val v_missingField =
        makeRecord(
          Value.ValueParty(alice),
          Value.ValueInt64(100),
        )

      val v_extendedWithNone =
        makeRecord(
          Value.ValueParty(alice),
          Value.ValueInt64(100),
          Value.ValueOptional(None),
        )

      val res = go("M2:do_fetch", v_missingField)

      inside(res) { case Right((v, _)) =>
        v shouldBe v_extendedWithNone
      }
    }

    "missing non-optional field -- should be rejected" in {

      val v_missingField =
        makeRecord(
          Value.ValueParty(alice),
          Value.ValueInt64(100),
        )

      val res = go("M1:do_fetch", v_missingField)
      inside(res) { case Left(err) =>
        err.toString should include(
          "Unexpected non-optional extra template field type encountered during upgrading: something is very wrong."
        )
      }
    }

  }

  "Correct calls to ResultNeedUpgradeVerification" - {

    val v_alice_bob =
      makeRecord(
        Value.ValueParty(alice),
        Value.ValueParty(bob),
      )

    "M3" in {
      val res = go("M3:do_fetch", v_alice_bob)
      inside(res) { case Right((v, List(uv))) =>
        v shouldBe v_alice_bob
        v shouldBe v_alice_bob
        uv.coid shouldBe theCid
        uv.signatories.toList shouldBe List(alice) // note difference with...
        uv.observers.toList shouldBe List()
        uv.keyOpt shouldBe None
      }
    }
    "M4" in {
      val res = go("M4:do_fetch", v_alice_bob)
      inside(res) { case Right((v, List(uv))) =>
        v shouldBe v_alice_bob
        uv.coid shouldBe theCid
        uv.signatories.toList shouldBe List(bob) // ..this
        uv.observers.toList shouldBe List()
        uv.keyOpt shouldBe None
      }
    }

  }

}
