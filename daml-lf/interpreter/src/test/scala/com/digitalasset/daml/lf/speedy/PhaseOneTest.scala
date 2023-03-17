// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref._
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast._
import com.daml.lf.language.PackageInterface

import com.daml.lf.speedy.ClosureConversion.closureConvert
import com.daml.lf.speedy.SExpr0._
import com.daml.lf.speedy.Anf.flattenToAnf

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.annotation.tailrec

class PhaseOneTest extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks {

  "compilation (stack-safety)" - {

    val phase1 = {
      def signatures: PartialFunction[PackageId, PackageSignature] = Map.empty
      def interface = new PackageInterface(signatures)
      def config =
        PhaseOne.Config(
          profiling = Compiler.NoProfile,
          stacktracing = Compiler.FullStackTrace,
        )
      new PhaseOne(interface, config)
    }

    // we test that increasing prefixes of the compilation pipeline are stack-safe

    def transform1(e: Expr): Boolean = {
      val _: SExpr = phase1.translateFromLF(PhaseOne.Env.Empty, e)
      true
    }

    def transform2(e: Expr): Boolean = {
      val e0: SExpr = phase1.translateFromLF(PhaseOne.Env.Empty, e)
      val _ = closureConvert(e0)
      true
    }

    def transform3(e: Expr): Boolean = {
      val e0: SExpr = phase1.translateFromLF(PhaseOne.Env.Empty, e)
      val e1 = closureConvert(e0)
      val _ = flattenToAnf(e1)
      true
    }

    /* We test stack-safety by building deep expressions through each of the different
     * recursion points of an expression, using one of the builder functions above, and
     * then ensuring we can 'transform' the expression by a prefix of the compilation.
     */
    def runTest(transform: Expr => Boolean)(depth: Int, cons: Expr => Expr): Boolean = {
      // Make an expression by iterating the 'cons' function, 'depth' times
      @tailrec def loop(x: Expr, n: Int): Expr = if (n == 0) x else loop(cons(x), n - 1)
      val source: Expr = loop(exp, depth)
      transform(source)
    }

    val testCases = {
      Table[String, Expr => Expr](
        ("name", "recursion-point"),
        ("tyApp", tyApp),
        ("app1", app1),
        ("app2", app2),
        ("app1of3", app1of3),
        ("app2of3", app2of3),
        ("app3of3", app3of3),
        ("esome", esome),
        ("eabs", eabs),
        ("etyabs", etyabs),
        ("struct1", struct1),
        ("struct2", struct2),
        ("consH", consH),
        ("consT", consT),
        ("scenPure", scenPure),
        ("scenBlock1", scenBlock1),
        ("scenBlock2", scenBlock2),
        ("scenCommit1", scenCommit1),
        ("scenCommit2", scenCommit2),
        ("scenMustFail1", scenMustFail1),
        ("scenMustFail2", scenMustFail2),
        ("scenPass", scenPass),
        ("scenParty", scenParty),
        ("scenEmbed", scenEmbed),
        ("upure", upure),
        ("ublock1", ublock1),
        ("ublock2", ublock2),
        ("ublock3", ublock3),
        ("ucreate", ucreate),
        ("ucreateI", ucreateI),
        ("ufetch", ufetch),
        ("ufetchI", ufetchI),
        ("uexercise1", uexercise1),
        ("uexercise2", uexercise2),
        ("uexerciseDynamic1", uexerciseDynamic1),
        ("uexerciseDynamic2", uexerciseDynamic2),
        ("uexerciseI1", uexerciseI1),
        ("uexerciseI2", uexerciseI2),
        ("uexerciseI3", uexerciseI3),
        ("uexerciseI4", uexerciseI4),
        ("uexerciseI5", uexerciseI5),
        ("uexbykey1", uexbykey1),
        ("uexbykey2", uexbykey2),
        ("ufetchbykey", ufetchbykey),
        ("ulookupbykey", ulookupbykey),
        ("uembed", uembed),
        ("utrycatch1", utrycatch1),
        ("utrycatch2", utrycatch2),
        ("structUpd1", structUpd1),
        ("structUpd2", structUpd2),
        ("recCon1", recCon1),
        ("recCon2", recCon2),
        ("caseScrut", caseScrut),
        ("caseAlt1", caseAlt1),
        ("caseAlt2", caseAlt2),
        ("let1", let1),
        ("let2", let2),
        ("eabs_esome", eabs_esome),
        ("etyabs_esome", etyabs_esome),
        ("app1_esome", app1_esome),
        ("app2_esome", app2_esome),
        ("tyApp_esome", tyApp_esome),
        ("let1_esome", let1_esome),
        ("let2_esome", let2_esome),
      )
    }

    {
      val depth = 10000 // 10k plenty to prove stack-safety (but we can do a million)
      s"transform(phase1), depth = $depth" - {
        forEvery(testCases) { (name: String, recursionPoint: Expr => Expr) =>
          name in {
            runTest(transform1)(depth, recursionPoint)
          }
        }
      }
    }

    {
      // TODO https://github.com/digital-asset/daml/issues/13351
      // The following testcases still appear quadratic during closure-conversion:
      //    scenBlock2, ublock2, ublock3
      val depth = 2000
      s"transform(phase1, closureConversion), depth = $depth" - {
        forEvery(testCases) { (name: String, recursionPoint: Expr => Expr) =>
          name in {
            runTest(transform2)(depth, recursionPoint)
          }
        }
      }
    }

    {
      // TODO https://github.com/digital-asset/daml/issues/13351
      val depth = 3000
      s"transform(phase1, closureConversion, flattenToAnf), depth = $depth" - {
        forEvery(testCases) { (name: String, recursionPoint: Expr => Expr) =>
          name in {
            runTest(transform3)(depth, recursionPoint)
          }
        }
      }
    }
  }

  // Construct one level of source-expression at various 'recursion-points'.
  private def app1 = (x: Expr) => EApp(x, exp)
  private def app2 = (x: Expr) => EApp(exp, x)
  private def app1of3 = (x: Expr) => EApp(x, EApp(exp, exp))
  private def app2of3 = (x: Expr) => EApp(exp, EApp(x, exp))
  private def app3of3 = (x: Expr) => EApp(exp, EApp(exp, x))
  private def tyApp = (x: Expr) => ETyApp(x, ty)
  private def esome = (x: Expr) => ESome(ty, x)
  private def eabs = (x: Expr) => EAbs(binder, x, None)
  private def etyabs = (x: Expr) => ETyAbs(tvBinder, x)
  private def struct1 = (x: Expr) => EStructCon(ImmArray((field, x), (field2, exp)))
  private def struct2 = (x: Expr) => EStructCon(ImmArray((field, exp), (field2, x)))
  private def structUpd1 = (x: Expr) => EStructUpd(field, x, exp)
  private def structUpd2 = (x: Expr) => EStructUpd(field, exp, x)
  private def consH = (x: Expr) => ECons(ty, ImmArray(x), exp)
  private def consT = (x: Expr) => ECons(ty, ImmArray(exp), x)
  private def scenPure = (x: Expr) => EScenario(ScenarioPure(ty, x))
  private def scenBlock1 = (x: Expr) =>
    EScenario(ScenarioBlock(ImmArray(Binding(None, ty, x)), exp))
  private def scenBlock2 = (x: Expr) =>
    EScenario(ScenarioBlock(ImmArray(Binding(None, ty, exp)), x))
  private def scenCommit1 = (x: Expr) => EScenario(ScenarioCommit(x, exp, ty))
  private def scenCommit2 = (x: Expr) => EScenario(ScenarioCommit(exp, x, ty))
  private def scenMustFail1 = (x: Expr) => EScenario(ScenarioMustFailAt(x, exp, ty))
  private def scenMustFail2 = (x: Expr) => EScenario(ScenarioMustFailAt(exp, x, ty))
  private def scenPass = (x: Expr) => EScenario(ScenarioPass(x))
  private def scenParty = (x: Expr) => EScenario(ScenarioGetParty(x))
  private def scenEmbed = (x: Expr) => EScenario(ScenarioEmbedExpr(ty, x))
  private def upure = (x: Expr) => EUpdate(UpdatePure(ty, x))
  private def ublock1 = (x: Expr) =>
    EUpdate(UpdateBlock(ImmArray(Binding(None, ty, x), Binding(None, ty, exp)), exp))
  private def ublock2 = (x: Expr) =>
    EUpdate(UpdateBlock(ImmArray(Binding(None, ty, exp), Binding(None, ty, x)), exp))
  private def ublock3 = (x: Expr) =>
    EUpdate(UpdateBlock(ImmArray(Binding(None, ty, exp), Binding(None, ty, exp)), x))
  private def ucreate = (x: Expr) => EUpdate(UpdateCreate(tcon, x))
  private def ucreateI = (x: Expr) => EUpdate(UpdateCreateInterface(tcon, x))
  private def ufetch = (x: Expr) => EUpdate(UpdateFetchTemplate(tcon, x))
  private def ufetchI = (x: Expr) => EUpdate(UpdateFetchInterface(tcon, x))
  private def uexercise1 = (x: Expr) => EUpdate(UpdateExercise(tcon, choice, x, exp))
  private def uexercise2 = (x: Expr) => EUpdate(UpdateExercise(tcon, choice, exp, x))
  private def uexerciseDynamic1 = (x: Expr) => EUpdate(UpdateDynamicExercise(tcon, choice, x, exp))
  private def uexerciseDynamic2 = (x: Expr) => EUpdate(UpdateDynamicExercise(tcon, choice, exp, x))
  private def uexerciseI1 = (x: Expr) =>
    EUpdate(UpdateExerciseInterface(tcon, choice, x, exp, Some(exp)))
  private def uexerciseI2 = (x: Expr) =>
    EUpdate(UpdateExerciseInterface(tcon, choice, exp, x, Some(exp)))
  private def uexerciseI3 = (x: Expr) =>
    EUpdate(UpdateExerciseInterface(tcon, choice, exp, exp, Some(x)))
  private def uexerciseI4 = (x: Expr) =>
    EUpdate(UpdateExerciseInterface(tcon, choice, x, exp, None))
  private def uexerciseI5 = (x: Expr) =>
    EUpdate(UpdateExerciseInterface(tcon, choice, exp, x, None))
  private def uexbykey1 = (x: Expr) => EUpdate(UpdateExerciseByKey(tcon, choice, x, exp))
  private def uexbykey2 = (x: Expr) => EUpdate(UpdateExerciseByKey(tcon, choice, exp, x))
  private def ufetchbykey = (x: Expr) => EUpdate(UpdateFetchByKey(RetrieveByKey(tcon, x)))
  private def ulookupbykey = (x: Expr) => EUpdate(UpdateLookupByKey(RetrieveByKey(tcon, x)))
  private def uembed = (x: Expr) => EUpdate(UpdateEmbedExpr(ty, x))
  private def utrycatch1 = (x: Expr) => EUpdate(UpdateTryCatch(ty, x, varname, exp))
  private def utrycatch2 = (x: Expr) => EUpdate(UpdateTryCatch(ty, exp, varname, x))

  private def recCon1 = (x: Expr) => ERecCon(tapp, ImmArray((field, x), (field2, exp)))
  private def recCon2 = (x: Expr) => ERecCon(tapp, ImmArray((field, exp), (field2, x)))

  // We dont test the recursion points ERecProj and ERecUpd, because the compiler requires
  // access to a package signature containing info for the type constructor.

  private def caseScrut = (x: Expr) => ECase(x, ImmArray())
  private def caseAlt1 = (x: Expr) => ECase(exp, ImmArray(CaseAlt(CPNil, x), alt))
  private def caseAlt2 = (x: Expr) => ECase(exp, ImmArray(alt, CaseAlt(CPNil, x)))
  private def let1 = (x: Expr) => ELet(Binding(None, ty, x), exp)
  private def let2 = (x: Expr) => ELet(Binding(None, ty, exp), x)

  // Alterating construction for applicaton & abstraction with some
  private def eabs_esome = (x: Expr) => eabs(esome(x))
  private def etyabs_esome = (x: Expr) => etyabs(esome(x))
  private def app1_esome = (x: Expr) => app1(esome(x))
  private def app2_esome = (x: Expr) => app2(esome(x))
  private def tyApp_esome = (x: Expr) => tyApp(esome(x))
  private def let1_esome = (x: Expr) => let1(esome(x))
  private def let2_esome = (x: Expr) => let2(esome(x))

  // Leaves for various types
  private def alt: CaseAlt = CaseAlt(CPNil, exp)
  private def exp: Expr = EPrimLit(PLText("exp"))
  private def ty: Type = TVar(Name.assertFromString("ty"))
  private def binder: (ExprVarName, Type) = (varname, ty)
  private def tvBinder: (TypeVarName, Kind) = (tvar, KStar)
  private def varname: ExprVarName = Name.assertFromString("x")
  private def tvar: TypeVarName = Name.assertFromString("T")
  private def tcon: TypeConName = Identifier.assertFromString("P:M:tcon")
  private def choice: ChoiceName = Name.assertFromString("choice")
  private def tapp: TypeConApp = TypeConApp(tcon, ImmArray.empty)
  private def field: FieldName = Name.assertFromString("field")
  private def field2: FieldName = Name.assertFromString("field2")
}
