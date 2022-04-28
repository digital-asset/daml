// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.language.PackageInterface

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.annotation.tailrec

class StackSafeTyping extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks {

  "typing (stack-safety)" - {

    val langVersion: LanguageVersion = LanguageVersion.default
    val signatures: PartialFunction[PackageId, PackageSignature] = Map.empty
    val interface = new PackageInterface(signatures)
    val ctx: Context = Context.None
    val env = Typing.Env(langVersion, interface, ctx)

    def typecheck(expr: Expr): Boolean = { // This is the code under test
      try {
        val _: Type = env.typeOf(expr)
        true
      } catch {
        case _: ValidationError => false
      }
    }

    /* We test stack-safety by building deep expressions through each of the different
     * recursion points of an expression, using one of the builder functions below, and
     * then ensuring we can 'typecheck' the expression.
     */
    def runTest(transform: Expr => Boolean)(depth: Int, cons: Expr => Expr): Boolean = {
      // Make an expression by iterating the 'cons' function, 'depth' times
      @tailrec def loop(x: Expr, n: Int): Expr = if (n == 0) x else loop(cons(x), n - 1)
      val source: Expr = loop(exp, depth)
      transform(source)
    }

    // TODO https://github.com/digital-asset/daml/issues/13351
    //
    // Add tests for recursion points in all syntactic classes which may recurse:
    // -- Kind, Type, Expr
    // And also for 'wide' lists, i.e. declaration lists

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
        ("uexerciseI1", uexerciseI1),
        ("uexerciseI2", uexerciseI2),
        ("uexerciseI3", uexerciseI3),
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
      val depth = 100 // Small enough to not cause stack-overflow, even for stack-unsafe code
      s"typing, (SMALL) depth = $depth" - {
        forEvery(testCases) { (name: String, recursionPoint: Expr => Expr) =>
          name in {
            runTest(typecheck)(depth, recursionPoint)
          }
        }
      }
    }

    {
      // TODO https://github.com/digital-asset/daml/issues/13351
      //
      // This following subset of testCases work already, even when the depth is LARGE.
      // The commented out examples fail because of stack-overflow, and demonstrate where
      // the Typing algorithm should be fixed to be stack safe.  (It might be the case
      // that the 'working' examples only work because they are ill-typed; it would be
      // good to setup deep/well-typed testcases for each recursion point)

      val testCases = {
        Table[String, Expr => Expr](
          ("name", "recursion-point"),
          ("tyApp", tyApp),
          //("app1", app1),
          ("app2", app2),
          //("app1of3", app1of3),
          ("app2of3", app2of3),
          ("app3of3", app3of3),
          ("esome", esome),
          ("eabs", eabs),
          //("etyabs", etyabs),
          //("struct1", struct1),
          //("struct2", struct2),
          ("consH", consH),
          ("consT", consT),
          ("scenPure", scenPure),
          ("scenBlock1", scenBlock1),
          ("scenBlock2", scenBlock2),
          ("scenCommit1", scenCommit1),
          ("scenCommit2", scenCommit2),
          ("scenMustFail1", scenMustFail1),
          ("scenMustFail2", scenMustFail2),
          //("scenPass", scenPass),
          //("scenParty", scenParty),
          //("scenEmbed", scenEmbed),
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
          //("uexerciseI1", uexerciseI1),
          ("uexerciseI2", uexerciseI2),
          ("uexerciseI3", uexerciseI3),
          //("uexbykey1", uexbykey1),
          ("uexbykey2", uexbykey2),
          ("ufetchbykey", ufetchbykey),
          ("ulookupbykey", ulookupbykey),
          //("uembed", uembed),
          ("utrycatch1", utrycatch1),
          ("utrycatch2", utrycatch2),
          //("structUpd1", structUpd1),
          ("structUpd2", structUpd2),
          ("recCon1", recCon1),
          ("recCon2", recCon2),
          //("caseScrut", caseScrut),
          ("caseAlt1", caseAlt1),
          ("caseAlt2", caseAlt2),
          ("let1", let1),
          //("let2", let2),
          ("eabs_esome", eabs_esome),
          ("etyabs_esome", etyabs_esome),
          ("app1_esome", app1_esome),
          ("app2_esome", app2_esome),
          ("tyApp_esome", tyApp_esome),
          ("let1_esome", let1_esome),
          ("let2_esome", let2_esome),
        )
      }

      val depth = 100000 // Big enough to provoke stack-overflow for stack-unsafe code
      s"typing, (LARGE) depth = $depth" - {
        forEvery(testCases) { (name: String, recursionPoint: Expr => Expr) =>
          name in {
            runTest(typecheck)(depth, recursionPoint)
          }
        }
      }
    }

  }

  // TODO: avoid duplication with code in PhaseOneTest.scala

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
  private def ufetch = (x: Expr) => EUpdate(UpdateFetch(tcon, x))
  private def ufetchI = (x: Expr) => EUpdate(UpdateFetchInterface(tcon, x))
  private def uexercise1 = (x: Expr) => EUpdate(UpdateExercise(tcon, choice, x, exp))
  private def uexercise2 = (x: Expr) => EUpdate(UpdateExercise(tcon, choice, exp, x))
  private def uexerciseI1 = (x: Expr) => EUpdate(UpdateExerciseInterface(tcon, choice, x, exp, exp))
  private def uexerciseI2 = (x: Expr) => EUpdate(UpdateExerciseInterface(tcon, choice, exp, x, exp))
  private def uexerciseI3 = (x: Expr) => EUpdate(UpdateExerciseInterface(tcon, choice, exp, exp, x))
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
