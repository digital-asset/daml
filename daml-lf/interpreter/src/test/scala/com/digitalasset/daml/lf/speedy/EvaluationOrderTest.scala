// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Location, Party}
import com.daml.lf.language.Ast._
import com.daml.lf.language.{PackageInterface}
import com.daml.lf.speedy.Compiler.FullStackTrace
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SResult._
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.validation.Validation
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.collection.mutable.ArrayBuffer

class TestTraceLog extends TraceLog {
  private val messages: ArrayBuffer[(String, Option[Location])] = new ArrayBuffer()

  override def add(message: String, optLocation: Option[Location]) = {
    messages += ((message, optLocation))
  }

  override def iterator = messages.iterator

  def getMessages: Seq[String] = messages.view.map(_._1).toSeq
}

class EvaluationOrderTest extends AnyWordSpec with Matchers {

  val pkgs: PureCompiledPackages = typeAndCompile(p"""
    module M {
      record @serializable T = { signatory : Party, observer : Party } ;
      template (this : T) = {
        precondition TRACE @Bool "precondition" True;
        signatories TRACE @(List Party) "signatories" (Cons @Party [M:T {signatory} this] (Nil @Party));
        observers TRACE @(List Party) "observers" (Cons @Party [M:T {observer} this] (Nil @Party));
        agreement TRACE @Text "agreement" "";
        key @Party
           (TRACE @Party "key" (M:T {signatory} this))
           (\(key : Party) -> TRACE @(List Party) "maintainers" (Cons @Party [key] (Nil @Party)));
      };
    }
  """)

  private val seed = crypto.Hash.hashPrivateKey("seed")

  private def evalUpdateApp(
      pkgs: CompiledPackages,
      e: Expr,
      args: Array[SValue],
      party: Party,
  ): (SResult, Seq[String]) = {
    val se = pkgs.compiler.unsafeCompile(e)
    val traceLog = new TestTraceLog()
    val res = Speedy.Machine
      .fromUpdateSExpr(pkgs, seed, SEApp(se, args.map(SEValue(_))), party, traceLog)
      .run()
    val msgs = traceLog.getMessages
    (res, msgs)
  }

  private val alice = Ref.Party.assertFromString("alice")
  private val bob = Ref.Party.assertFromString("bob")

  "evaluation order" should {
    "evaluate in correct order for successful create" in {
      val (res, msgs) = evalUpdateApp(
        pkgs,
        e"\(sig : Party) (obs : Party) -> create @M:T M:T { signatory = sig, observer = obs }",
        Array(SParty(alice), SParty(bob)),
        alice,
      )
      res shouldBe a[SResultFinalValue]
      msgs shouldBe Seq(
        "precondition",
        "agreement",
        "signatories",
        "observers",
        "key",
        "maintainers",
      )
      succeed
    }
  }

  private def typeAndCompile(pkg: Package): PureCompiledPackages = {
    import defaultParserParameters.defaultPackageId
    val rawPkgs = Map(defaultPackageId -> pkg)
    Validation.checkPackage(PackageInterface(rawPkgs), defaultPackageId, pkg)
    val compilerConfig = Compiler.Config.Dev.copy(stacktracing = FullStackTrace)
    PureCompiledPackages.assertBuild(rawPkgs, compilerConfig)
  }
}
