// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref._
import com.daml.lf.data.ImmArray
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.testing.parser.Implicits.{defaultParserParameters => _, _}
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.{TransactionVersion, Versioned}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext

import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.util._ //{Success, Try}

import com.daml.lf.transaction.TransactionVersion.VDev

class UpgradeTest extends AnyFreeSpec with Matchers with Inside {

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

      record @serializable T1 = { theSig: Party, f1: Text, f2: Text};
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
  """,
  )

  private val pkgs = {

    val packageMap = Map(
      pid1 -> package1
    )

    PureCompiledPackages.assertBuild(
      packageMap,
      Compiler.Config.Dev.copy(
        enableContractUpgrading = true
      ),
    )
  }

  private val List(alice) = List("alice").map(Party.assertFromString)

  private def evalUpdateApp(
      pkgs: CompiledPackages,
      e: Expr,
      args: Array[SValue],
      getContract: Map[Value.ContractId, Value.VersionedContractInstance],
  ): Try[Either[SError, SValue]] = {

    val seed = crypto.Hash.hashPrivateKey("seed")

    implicit def logContext: LoggingContext = LoggingContext.ForTesting

    val se = pkgs.compiler.unsafeCompile(e)
    val machine = Speedy.Machine
      .fromUpdateSExpr(
        pkgs,
        seed,
        if (args.isEmpty) se else SEApp(se, args),
        Set(alice),
      )
    Try( // NICK, why Try?
      SpeedyTestLib.run(
        machine,
        getContract = getContract,
      )
    )
  }

  val theCid = Value.ContractId.V1(crypto.Hash.hashPrivateKey(s"theCid"))

  val tycon: Identifier = {
    val P1_M1_Tx = parseTyCon(pid1, "M1:Tx")
    P1_M1_Tx
  }

  "downgrade attempted" - {

    // These test check downgrade of different shapea actula contract
    // when we are expecting a contract of type M1:Tx

    val v1_base: Value =
      Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueParty(alice),
          None -> Value.ValueText("bar"),
          None -> Value.ValueText("foo"),
        ),
      )

    "correct fields" in {

      val contract: Versioned[Value.ContractInstance] =
        Versioned(
          TransactionVersion.StableVersions.max,
          Value.ContractInstance(
            tycon,
            v1_base,
          ),
        )

      val res = evalUpdateApp(
        pkgs,
        parseExpr(pid1, "M1:do_fetch"),
        Array(SContractId(theCid)),
        Map(theCid -> contract),
      )
      // println(s"res=$res") // NICK
      inside(res) { case Success(x) =>
        inside(x) { case Right(sv) =>
          val _: SValue = sv
          val v = sv.toNormalizedValue(VDev)
          v shouldBe v1_base
        }
      }

    }

    "extra field (text) - something is very wrong" in {

      val v1_extraText: Value =
        Value.ValueRecord(
          None,
          ImmArray(
            None -> Value.ValueParty(alice),
            None -> Value.ValueText("bar"),
            None -> Value.ValueText("foo"),
            None -> Value.ValueText("extra"),
          ),
        )

      val contract: Versioned[Value.ContractInstance] =
        Versioned(
          TransactionVersion.StableVersions.max,
          Value.ContractInstance(
            tycon,
            v1_extraText,
          ),
        )

      val res = evalUpdateApp(
        pkgs,
        parseExpr(pid1, "M1:do_fetch"),
        Array(SContractId(theCid)),
        Map(theCid -> contract),
      )

      inside(res) { case Success(x) =>
        inside(x) { case Left(err) =>
          err.toString should include(
            "Unexpected non-optional extra contract field encountered during downgrading: something is very wrong."
          )
        }
      }

    }

    "extra field (Some) - cannot be dropped" in {

      val v1_extraSome: Value =
        Value.ValueRecord(
          None,
          ImmArray(
            None -> Value.ValueParty(alice),
            None -> Value.ValueText("bar"),
            None -> Value.ValueText("foo"),
            None -> Value.ValueOptional(Some(Value.ValueText("heyhey"))),
          ),
        )

      val contract: Versioned[Value.ContractInstance] =
        Versioned(
          TransactionVersion.StableVersions.max,
          Value.ContractInstance(
            tycon,
            v1_extraSome,
          ),
        )

      val res = evalUpdateApp(
        pkgs,
        parseExpr(pid1, "M1:do_fetch"),
        Array(SContractId(theCid)),
        Map(theCid -> contract),
      )
      inside(res) { case Success(x) =>
        inside(x) { case Left(err) =>
          err.toString should include(
            "An optional contract field with a value of Some may not be dropped during downgrading"
          )
        }
      }

    }

    "extra field (None) - OK, downgrade allowed" in {

      val v1_extraNone: Value =
        Value.ValueRecord(
          None,
          ImmArray(
            None -> Value.ValueParty(alice),
            None -> Value.ValueText("bar"),
            None -> Value.ValueText("foo"),
            None -> Value.ValueOptional(None),
          ),
        )

      val contract: Versioned[Value.ContractInstance] =
        Versioned(
          TransactionVersion.StableVersions.max,
          Value.ContractInstance(
            tycon,
            v1_extraNone,
          ),
        )

      val res = evalUpdateApp(
        pkgs,
        parseExpr(pid1, "M1:do_fetch"),
        Array(SContractId(theCid)),
        Map(theCid -> contract),
      )

      inside(res) { case Success(x) =>
        inside(x) { case Right(sv) =>
          val _: SValue = sv
          val v = sv.toNormalizedValue(VDev)
          v shouldBe v1_base
        }
      }

    }

  }
}
