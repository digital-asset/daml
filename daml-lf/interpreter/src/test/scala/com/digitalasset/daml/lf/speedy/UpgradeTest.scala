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

  private val packageMap = Map(
    pid1 -> package1
  )

  private val pkgs = {
    PureCompiledPackages.assertBuild(
      packageMap,
      Compiler.Config.Dev.copy(
        enableContractUpgrading = true
      ),
    )
  }

  private val List(alice) = List("alice").map(Party.assertFromString)

  private val seed = crypto.Hash.hashPrivateKey("seed")

  private def evalUpdateApp(
      pkgs: CompiledPackages,
      e: Expr,
      args: Array[SValue],
      getContract: Map[Value.ContractId, Value.VersionedContractInstance],
  ): Try[Either[SError, SValue]] = {

    implicit def logContext: LoggingContext = LoggingContext.ForTesting

    val se = pkgs.compiler.unsafeCompile(e)
    val machine = Speedy.Machine
      .fromUpdateSExpr(
        pkgs,
        seed,
        if (args.isEmpty) se else SEApp(se, args),
        Set(alice),
      )
    Try(
      SpeedyTestLib.run(
        machine,
        getContract = getContract,
      )
    )
  }

  val P1_M1_Tx = parseTyCon(pid1, "M1:Tx")
  val theCid = Value.ContractId.V1(crypto.Hash.hashPrivateKey(s"theCid"))

  val tycon: Identifier = P1_M1_Tx

  "upgrades and downgrades" - {

    "correct fields" in {
      val contract: Versioned[Value.ContractInstance] = {
        Versioned(
          TransactionVersion.StableVersions.max,
          Value.ContractInstance(
            tycon,
            Value.ValueRecord(
              None,
              ImmArray(
                Some(n"theSig") -> Value.ValueParty(alice),
                Some(n"f1") -> Value.ValueText("bar"),
                Some(n"f2") -> Value.ValueText("foo"),
              ),
            ),
          ),
        )
      }
      val res = evalUpdateApp(
        pkgs,
        parseExpr(pid1, "M1:do_fetch"),
        Array(SContractId(theCid)),
        Map(theCid -> contract),
      )
      println(s"res=$res")
    }

    "extra field (text) - BAD" in {
      val contract: Versioned[Value.ContractInstance] = {
        Versioned(
          TransactionVersion.StableVersions.max,
          Value.ContractInstance(
            tycon,
            Value.ValueRecord(
              None,
              ImmArray(
                Some(n"theSig") -> Value.ValueParty(alice),
                Some(n"f1") -> Value.ValueText("bar"),
                Some(n"f2") -> Value.ValueText("foo"),
                Some(n"smee") -> Value.ValueText("extra"),
              ),
            ),
          ),
        )
      }
      val res = evalUpdateApp(
        pkgs,
        parseExpr(pid1, "M1:do_fetch"),
        Array(SContractId(theCid)),
        Map(theCid -> contract),
      )
      println(s"res=$res")
    }

    "extra field (Some) - cannot be dropped" in {
      val contract: Versioned[Value.ContractInstance] = {
        Versioned(
          TransactionVersion.StableVersions.max,
          Value.ContractInstance(
            tycon,
            Value.ValueRecord(
              None,
              ImmArray(
                Some(n"theSig") -> Value.ValueParty(alice),
                Some(n"f1") -> Value.ValueText("bar"),
                Some(n"f2") -> Value.ValueText("foo"),
                Some(n"smee") -> Value.ValueOptional(Some(Value.ValueText("heyhey"))),
              ),
            ),
          ),
        )
      }
      val res = evalUpdateApp(
        pkgs,
        parseExpr(pid1, "M1:do_fetch"),
        Array(SContractId(theCid)),
        Map(theCid -> contract),
      )
      println(s"res=$res")
    }

    "extra field (None) - OK, downgrade allowed" in {
      val contract: Versioned[Value.ContractInstance] = {
        Versioned(
          TransactionVersion.StableVersions.max,
          Value.ContractInstance(
            tycon,
            Value.ValueRecord(
              None,
              ImmArray(
                Some(n"theSig") -> Value.ValueParty(alice),
                Some(n"f1") -> Value.ValueText("bar"),
                Some(n"f2") -> Value.ValueText("foo"),
                Some(n"smee") -> Value.ValueOptional(None),
              ),
            ),
          ),
        )
      }
      val res = evalUpdateApp(
        pkgs,
        parseExpr(pid1, "M1:do_fetch"),
        Array(SContractId(theCid)),
        Map(theCid -> contract),
      )
      println(s"res=$res")
    }

  }
}
