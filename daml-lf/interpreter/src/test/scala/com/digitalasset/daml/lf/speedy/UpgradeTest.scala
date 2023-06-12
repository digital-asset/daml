// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util

import com.daml.lf.data.Ref._
import com.daml.lf.data.ImmArray
import com.daml.lf.interpretation.{Error => IE}
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.language.PackageInterface
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.testing.parser.Implicits.{defaultParserParameters => _, _}
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.{TransactionVersion, Versioned}
import com.daml.lf.validation.Validation
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext

import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Success, Try}

class UpgradeTest extends AnyFreeSpec with Matchers with Inside {

  private def makePP(pid: PackageId): ParserParameters[this.type] = {
    ParserParameters(pid, languageVersion = LanguageVersion.v1_dev)
  }

  private def parType(pid: PackageId, s: String): Type = {
    implicit val pp: ParserParameters[this.type] = makePP(pid)
    t"$s"
  }

  private def parExpr(pid: PackageId, s: String): Expr = {
    implicit val pp: ParserParameters[this.type] = makePP(pid)
    e"$s"
  }

  private def parPackage(pid: PackageId, s: String): Package = {
    implicit val pp: ParserParameters[this.type] = makePP(pid)
    p"$s"
  }

  private def parTyCon(pid: PackageId, s: String): Identifier = {
    parType(pid, s) match {
      case TTyCon(tycon) => tycon
      case _ => sys.error("unexpect error")
    }
  }

  val genCid: () => Value.ContractId = {
    var n = 0
    () => {
      n = n + 1;
      Value.ContractId.V1(crypto.Hash.hashPrivateKey(s"cid-$n"))
    }
  }

  // P1
  // Defines "base" type (M1.T1) which tests will upgrade or downgrade to.
  // Defines similar types (M1.T2 and (M2.T1) which are not convertable
  // Defines fetch entry point
  private val pid1: PackageId = PackageId.assertFromString("P1")
  private val package1: Package = parPackage(
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

      record @serializable T2 = { theSig: Party, f1: Text, f2: Text};
      template (this: T2) = {
        precondition True;
        signatories Cons @Party [M1:T2 {theSig} this] Nil @Party;
        observers Nil @Party;
        agreement "Agreement";
      };

      val do_fetch_t1: ContractId M1:T1 -> Update M1:T1 =
        \(cId: ContractId M1:T1) ->
          fetch_template @M1:T1 cId;

    }
  """,
  )

  // P2
  // Defines M1.T1, which is structurally identical to that in P1
  private val pid2: PackageId = PackageId.assertFromString("P2")
  private val package2: Package = parPackage(
    pid2,
    """
    module M1 {

      record @serializable T1 = { theSig: Party, f1: Text, f2: Text};
      template (this: T1) = {
        precondition True;
        signatories Cons @Party [M1:T1 {theSig} this] Nil @Party;
        observers Nil @Party;
        agreement "Agreement";
      };

    }
  """,
  )

  // P3
  // Defines M1.T1, which is structurally identical to that in P1 (except reordered fields)
  private val pid3: PackageId = PackageId.assertFromString("P3")
  private val package3: Package = parPackage(
    pid3,
    """
    module M1 {

      record @serializable T1 = { theSig: Party, f2: Text, f1: Text}; //reordered
      template (this: T1) = {
        precondition True;
        signatories Cons @Party [M1:T1 {theSig} this] Nil @Party;
        observers Nil @Party;
        agreement "Agreement";
      };

    }
  """,
  )

  private val packageMap = Map(
    pid1 -> package1,
    pid2 -> package2,
    pid3 -> package3,
  )

  private val pkgs = {
    Validation.unsafeCheckPackages(PackageInterface(packageMap), packageMap)
    PureCompiledPackages.assertBuild(
      packageMap,
      Compiler.Config.Dev.copy(stacktracing = Compiler.FullStackTrace),
    )
  }

  private val List(alice) = List("alice").map(Party.assertFromString)

  private def buildContract(tycon: Identifier): Versioned[Value.ContractInstance] = {
    Versioned(
      TransactionVersion.StableVersions.max,
      Value.ContractInstance(
        tycon,
        Value.ValueRecord(
          None,
          ImmArray(
            // NICK: dont use None here for field names
            None -> Value.ValueParty(alice),
            None -> Value.ValueText("foo"),
            None -> Value.ValueText("bar"),
          ),
        ),
      ),
    )
  }

  private val P1_M1_T1 = parTyCon(pid1, "M1:T1")
  private val P1_M1_T2 = parTyCon(pid1, "M1:T2")
  private val P2_M1_T1 = parTyCon(pid2, "M1:T1")
  private val P3_M1_T1 = parTyCon(pid3, "M1:T1")

  private val cid_P1_M1_T1 = genCid()
  private val cid_P1_M1_T2 = genCid()
  private val cid_P2_M1_T1 = genCid()
  private val cid_P3_M1_T1 = genCid()

  private val getContract =
    Map(
      cid_P1_M1_T1 -> buildContract(P1_M1_T1),
      cid_P1_M1_T2 -> buildContract(P1_M1_T2),
      cid_P2_M1_T1 -> buildContract(P2_M1_T1),
      cid_P3_M1_T1 -> buildContract(P3_M1_T1),
    )

  private val seed = crypto.Hash.hashPrivateKey("seed")

  private def evalUpdateApp(
      pkgs: CompiledPackages,
      e: Expr,
      args: Array[SValue],
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

  /*
   NICK: Here are the features we wish to support. Drive with unit tests...

   done: Same Pid; Same Qualified typename -- ACCEPT (no upgrade required)
   done: Same Pid; Same module-name; Different typecon - REJECT
   TODO: Same Pid; Different module-name; Same typecon - REJECT

   (Different Pid; Same Qualified type-name)...

   done: exactly the same field names & types -- ACCEPT
   TODO: fields have different type -- REJECT
   TODO: same field names & types (reordered) -- ACCEPT

   TODO: fields match except for extra fields with optional type -- ACCEPT (upgrade)
   TODO: fields match except for missing fields -- ACCEPT (downgrade)
   TODO: extra fields with optional type & missing fields -- ACCEPT (upgrade+downgrade)
   */

  "upgrades and downgrades" - {

    // Same Pid; Same Qualified typename -- ACCEPT (no upgrade required)
    "P1.M1.T1 fetched as P1.M1.T1 [ACCEPT]" in {
      val res = evalUpdateApp(
        pkgs,
        parExpr(pid1, "M1:do_fetch_t1"),
        Array(SContractId(cid_P1_M1_T1)),
      )
      inside(res) { case Success(Right(fetched)) =>
        val t1_fieldNames: ImmArray[Name] = ImmArray(n"theSig", n"f1", n"f2")
        val t1_fields: util.ArrayList[SValue] = ArrayList(SParty(alice), SText("foo"), SText("bar"))
        val expected = SRecord(id = P1_M1_T1, t1_fieldNames, t1_fields)
        println(s"fetched=$fetched") // NICK
        println(s"expected=$expected") // NICK
        fetched shouldBe expected
      }
    }

    // Same Pid; Same module-name; Different typecon - REJECT
    "P1.M1.T2 fetched as P1.M1.T1 [REJECT]" in {
      val res = evalUpdateApp(
        pkgs,
        parExpr(pid1, "M1:do_fetch_t1"),
        Array(SContractId(cid_P1_M1_T2)),
      )
      inside(res) {
        case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, P1_M1_T1, P1_M1_T2)))) =>
      }
    }

    // (Different Pid) exactly the same field names & types -- ACCEPT
    "P2.M1.T1 fetched as P1.M1.T1 [ACCEPT]" in {
      val res = evalUpdateApp(
        pkgs,
        parExpr(pid1, "M1:do_fetch_t1"),
        Array(SContractId(cid_P2_M1_T1)),
      )

      inside(res) { case Success(Right(fetched)) =>
        val t1_fieldNames: ImmArray[Name] = ImmArray(n"theSig", n"f1", n"f2")
        val t1_fields: util.ArrayList[SValue] = ArrayList(SParty(alice), SText("foo"), SText("bar"))
        val expected = SRecord(id = P1_M1_T1, t1_fieldNames, t1_fields)
        println(s"fetched=$fetched") // NICK
        println(s"expected=$expected") // NICK
        fetched shouldBe expected
      }
    }

    // (Different Pid) same field names & types (reordered) -- ACCEPT
    "P3.M1.T1 fetched as P1.M1.T1 [success]" in {
      val res = evalUpdateApp(
        pkgs,
        parExpr(pid1, "M1:do_fetch_t1"),
        Array(SContractId(cid_P3_M1_T1)),
      )

      inside(res) { case Success(Right(fetched)) =>
        val t1_fieldNames: ImmArray[Name] = ImmArray(n"theSig", n"f1", n"f2")
        val t1_fields: util.ArrayList[SValue] = ArrayList(SParty(alice), SText("foo"), SText("bar"))
        val expected = SRecord(id = P1_M1_T1, t1_fieldNames, t1_fields)
        println(s"fetched=$fetched") // NICK
        println(s"expected=$expected") // NICK
      // fetched shouldBe expected // NICK: make this fail for the right reason!
      }
    }

  }
}
