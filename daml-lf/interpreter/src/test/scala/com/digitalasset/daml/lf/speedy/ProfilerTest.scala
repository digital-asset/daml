// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SResult._
import com.daml.lf.testing.parser.Implicits._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.{ScalaCheckDrivenPropertyChecks}
import scala.jdk.CollectionConverters._

class ProfilerTest extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import SpeedyTestLib.loggingContext
  import defaultParserParameters.{defaultPackageId => pkgId}

  private[this] val pkg =
    p"""
        module M {

          record @serializable T = { party: Party };

          template (this : T) =  {
            precondition True;
            signatories Cons @Party [M:T {party} this] (Nil @Party);
            observers Nil @Party;
            agreement "";
            choice Ch1 (self) (i : Unit) : Unit,
              controllers Cons @Party [M:T {party} this] (Nil @Party)
              to
                ubind
                  x1: ContractId M:T <- create @M:T M:T { party = M:T {party} this };
                  x2: ContractId M:T <- create @M:T M:T { party = M:T {party} this }
                in upure @Unit ();
          };

          val exp1 : Party -> Update Unit = \(party: Party) ->
              ubind
                x1: ContractId M:T <- create @M:T M:T { party = party };
                u: Unit <- exercise @M:T Ch1 x1 ()
              in upure @Unit ();

          val argOrder : Party -> Update Unit = \(party: Party) ->
            let f: Int64 -> Int64 = \(x: Int64) -> ADD_INT64 x 1 in
            let g: Int64 -> Int64 = \(x: Int64) -> ADD_INT64 x 2 in
            let x: Int64 = f (g 1) in
            upure @Unit ();
        }
    """

  val config = Compiler.Config.Default.copy(profiling = Compiler.FullProfile)
  val compiledPackages = PureCompiledPackages.assertBuild(Map(pkgId -> pkg), config)

  private def id(s: String) =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString(s"M:$s"))
  private def c(s: String) =
    Ref.ChoiceName.assertFromString(s)
  private def v(s: String) = Ref.Name.assertFromString(s)

  def profile(e: Expr): Seq[(Boolean, Profile.Label)] = {
    val transactionSeed: crypto.Hash = crypto.Hash.hashPrivateKey("foobar")
    val party = Ref.Party.assertFromString("Alice")
    val se = compiledPackages.compiler.unsafeCompile(e)
    val example: SExpr = SEApp(se, Array(SParty(party)))
    val machine =
      Speedy.Machine.fromUpdateSExpr(compiledPackages, transactionSeed, example, Set(party))
    val res = machine.run()
    res match {
      case SResultFinal(_) =>
        machine.profile.events.asScala.toList.map(ev => (ev.open, ev.rawLabel))
      case _ =>
        sys.error(s"Unexpected res: $res")
    }
  }

  "profiler" should {

    "include create and exercise events" in {
      // Note that exp1 closes before we get to the choices. This is because
      // exp1 is an expression of the form \party -> \ token -> …
      // We only profile the outer closure here which stops before we get to the token.
      profile(e"M:exp1") shouldBe
        List(
          (true, LfDefRef(id("exp1"))),
          (false, LfDefRef(id("exp1"))),
          (true, CreateDefRef(id("T"))),
          (true, TemplatePreConditionDefRef(id("T"))),
          (false, TemplatePreConditionDefRef(id("T"))),
          (true, SignatoriesDefRef(id("T"))),
          (false, SignatoriesDefRef(id("T"))),
          (true, ObserversDefRef(id("T"))),
          (false, ObserversDefRef(id("T"))),
          (false, CreateDefRef(id("T"))),
          (true, TemplateChoiceDefRef(id("T"), c("Ch1"))),
          (true, CreateDefRef(id("T"))),
          (true, TemplatePreConditionDefRef(id("T"))),
          (false, TemplatePreConditionDefRef(id("T"))),
          (true, SignatoriesDefRef(id("T"))),
          (false, SignatoriesDefRef(id("T"))),
          (true, ObserversDefRef(id("T"))),
          (false, ObserversDefRef(id("T"))),
          (false, CreateDefRef(id("T"))),
          (true, CreateDefRef(id("T"))),
          (true, TemplatePreConditionDefRef(id("T"))),
          (false, TemplatePreConditionDefRef(id("T"))),
          (true, SignatoriesDefRef(id("T"))),
          (false, SignatoriesDefRef(id("T"))),
          (true, ObserversDefRef(id("T"))),
          (false, ObserversDefRef(id("T"))),
          (false, CreateDefRef(id("T"))),
          (false, TemplateChoiceDefRef(id("T"), c("Ch1"))),
        )
    }
    "evaluate arguments before open event" in {
      profile(e"M:argOrder") shouldBe List[(Boolean, Profile.Label)](
        (true, LfDefRef(id("argOrder"))),
        (true, v("g")),
        (false, v("g")),
        (true, v("f")),
        (false, v("f")),
        (false, LfDefRef(id("argOrder"))),
      )
    }
  }
}
