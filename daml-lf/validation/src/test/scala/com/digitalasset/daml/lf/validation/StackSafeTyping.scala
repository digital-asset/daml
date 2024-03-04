// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref._
import com.daml.lf.data.Struct
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.language.PackageInterface

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.annotation.tailrec

class StackSafeTyping extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks {

  def unitT: Type = TBuiltin(BTUnit)
  def theType: Type = unitT
  def arrow(ty1: Type, ty2: Type): Type = TApp(TApp(TBuiltin(BTArrow), ty1), ty2)
  def tyvar: TypeVarName = Name.assertFromString("T")

  def field: FieldName = Name.assertFromString("field")
  def field2: FieldName = Name.assertFromString("field2")
  def makeStruct(ty1: Type, ty2: Type): Type = TStruct(
    Struct.assertFromSeq(List((field, ty1), (field2, ty2)))
  )

  def forall(x: Type): Type = TForall((tyvar, KStar), x)
  def arrowRight(x: Type) = arrow(theType, x)
  def arrowLeft(x: Type) = arrow(x, theType)
  def struct1(x: Type) = makeStruct(x, theType)
  def struct2(x: Type) = makeStruct(theType, x)

  /* We test stack-safety by building deep expressions through different recursion points
   * of an AST, then check we can run the 'code under test', without blowing the stack.
   */
  def makeAst[Ast](depth: Int, init: Ast, cons: Ast => Ast): Ast = {
    // Make an AST by iterating the 'cons' function, 'depth' times, starting from 'init'
    @tailrec def loop(x: Ast, n: Int): Ast = if (n == 0) x else loop(cons(x), n - 1)
    val ast: Ast = loop(init, depth)
    ast
  }

  "alpha equivalence (stack-safety)" - {

    val testCases = {
      Table[String, Type => Type](
        ("name", "recursion-point"),
        ("forall", forall),
        ("arrowRight", arrowRight),
        ("arrowLeft", arrowLeft),
        ("struct1", struct1),
        ("struct2", struct2),
      )
    }
    {
      val depth = 10000
      s"alpha equivalence, (BIG) depth = $depth" - {
        forEvery(testCases) { (name: String, recursionPoint: Type => Type) =>
          name in {
            val a = Name.assertFromString("A")
            val b = Name.assertFromString("B")
            val tyA = TForall((a, KStar), makeAst(depth, TVar(a), recursionPoint))
            val tyB = TForall((b, KStar), makeAst(depth, TVar(b), recursionPoint))
            AlphaEquiv.alphaEquiv(tyA, tyB) shouldBe true
          }
        }
      }
    }
  }

  "kind checking (stack-safety)" - {

    // This is the code under test...
    def kindCheck(typ: Type): Option[ValidationError] = {
      val langVersion: LanguageVersion = LanguageVersion.default
      val signatures: PartialFunction[PackageId, PackageSignature] = Map.empty
      val pkgInterface = new PackageInterface(signatures)
      val ctx: Context = Context.None
      val env = Typing.Env(langVersion, pkgInterface, ctx)
      try {
        val _: Kind = env.kindOf(typ)
        None
      } catch {
        case e: ValidationError => Some(e)
      }
    }

    val testCases = {
      Table[String, Type => Type](
        ("name", "recursion-point"),
        ("forall", forall),
        ("arrowRight", arrowRight),
        ("arrowLeft", arrowLeft),
        ("struct1", struct1),
        ("struct2", struct2),
      )
    }
    {
      val depth = 10000
      s"kind checking, (LARGE) depth = $depth" - {
        forEvery(testCases) { (name: String, recursionPoint: Type => Type) =>
          name in {
            // ensure examples can be kind-checked and are well-kinded
            kindCheck(makeAst(depth, theType, recursionPoint)) shouldBe None
          }
        }
      }
    }
  }

  "typing (stack-safety)" - {

    // types

    def textT: Type = TBuiltin(BTText)
    def intT: Type = TBuiltin(BTInt64)
    def partyT: Type = TBuiltin(BTParty)
    def timestampT: Type = TBuiltin(BTTimestamp)
    def unitT: Type = TBuiltin(BTUnit)
    def listT: Type = TApp(TBuiltin(BTList), theType)
    def scenT: Type = scenario(theType)
    def updateT: Type = TApp(TBuiltin(BTUpdate), theType)
    def scenario(ty: Type): Type = TApp(TBuiltin(BTScenario), ty)
    def optional(ty: Type): Type = TApp(TBuiltin(BTOptional), ty)
    def arrow(ty1: Type, ty2: Type): Type = TApp(TApp(TBuiltin(BTArrow), ty1), ty2)
    def structT: Type = TStruct(
      Struct.assertFromSeq(List((field, theType), (field2, theType)))
    )

    // make an expression of any given type...
    def mk(ty: Type): Expr = {
      EApp(ETyApp(EBuiltinFun(BError), ty), EBuiltinLit(BLText("message")))
    }

    def theType: Type = unitT
    def theExp: Expr = mk(theType)

    // embed an expression of the fixed type, whilst constructing an expression of any given type
    def embed(exp: Expr, ty: Type): Expr = {
      EApp(mk(arrow(theType, ty)), exp)
    }
    // consume an expression of any given type, whilst constructing an expression of the fixed type
    def consume(ty: Type, exp: Expr): Expr = {
      EApp(mk(arrow(ty, theType)), exp)
    }

    def var1: ExprVarName = Name.assertFromString("x1")
    def var2: ExprVarName = Name.assertFromString("x2")
    def tyvar: TypeVarName = Name.assertFromString("T")
    def field: FieldName = Name.assertFromString("field")
    def field2: FieldName = Name.assertFromString("field2")

    // Construct one level of source-expression at various 'recursion-points'...

    // The constructed expressions *must* be well-typed, or else the type-checking
    // algorithm will terminate early, and hence stack-safety is not tested.

    def app1 = (x: Expr) => EApp(embed(x, arrow(theType, theType)), theExp)
    def app2 = (x: Expr) => EApp(mk(arrow(theType, theType)), x)
    def tyApp = (x: Expr) => ETyApp(embed(x, TForall((tyvar, KStar), theType)), theType)
    def esome = (x: Expr) => consume(optional(theType), ESome(theType, x))
    def eabs = (x: Expr) => EAbs((var1, theType), x, None)
    def etyabs = (x: Expr) => ETyAbs((tyvar, KStar), x)
    def struct1 = (x: Expr) => EStructCon(ImmArray((field, x), (field2, theExp)))
    def struct2 = (x: Expr) => EStructCon(ImmArray((field, theExp), (field2, x)))
    def consH = (x: Expr) => consume(listT, ECons(theType, ImmArray(x), mk(listT)))
    def consT = (x: Expr) => consume(listT, ECons(theType, ImmArray(theExp), embed(x, listT)))
    def scenPure = (x: Expr) => consume(scenT, EScenario(ScenarioPure(theType, x)))
    def scenBlock1 = (x: Expr) =>
      consume(
        scenT,
        EScenario(ScenarioBlock(ImmArray(Binding(None, theType, embed(x, scenT))), mk(scenT))),
      )
    def scenBlock2 = (x: Expr) =>
      consume(
        scenT,
        EScenario(ScenarioBlock(ImmArray(Binding(None, theType, mk(scenT))), embed(x, scenT))),
      )
    def scenCommit1 =
      (x: Expr) => consume(scenT, EScenario(ScenarioCommit(embed(x, partyT), mk(updateT), theType)))
    def scenCommit2 =
      (x: Expr) => consume(scenT, EScenario(ScenarioCommit(mk(partyT), embed(x, updateT), theType)))
    def scenMustFail1 = (x: Expr) =>
      consume(scenT, EScenario(ScenarioMustFailAt(embed(x, partyT), mk(updateT), theType)))
    def scenMustFail2 = (x: Expr) =>
      consume(scenT, EScenario(ScenarioMustFailAt(mk(partyT), embed(x, updateT), theType)))
    def scenPass =
      (x: Expr) => consume(scenario(timestampT), EScenario(ScenarioPass(embed(x, intT))))
    def scenParty =
      (x: Expr) => consume(scenario(partyT), EScenario(ScenarioGetParty(embed(x, textT))))
    def scenEmbed = (x: Expr) =>
      consume(scenario(intT), EScenario(ScenarioEmbedExpr(intT, embed(x, scenario(intT)))))
    def upure = (x: Expr) => consume(updateT, EUpdate(UpdatePure(theType, x)))
    def ublock1 = (x: Expr) =>
      consume(
        updateT,
        EUpdate(
          UpdateBlock(
            ImmArray(Binding(None, unitT, embed(x, updateT)), Binding(None, unitT, mk(updateT))),
            mk(updateT),
          )
        ),
      )
    def ublock2 = (x: Expr) =>
      consume(
        updateT,
        EUpdate(
          UpdateBlock(
            ImmArray(Binding(None, unitT, mk(updateT)), Binding(None, unitT, embed(x, updateT))),
            mk(updateT),
          )
        ),
      )
    def ublock3 = (x: Expr) =>
      consume(
        updateT,
        EUpdate(
          UpdateBlock(
            ImmArray(Binding(None, unitT, mk(updateT)), Binding(None, unitT, mk(updateT))),
            embed(x, updateT),
          )
        ),
      )
    def uembed = (x: Expr) => consume(updateT, EUpdate(UpdateEmbedExpr(unitT, embed(x, updateT))))
    def utrycatch1 = (x: Expr) =>
      consume(
        updateT,
        EUpdate(UpdateTryCatch(unitT, embed(x, updateT), var1, mk(optional(updateT)))),
      )
    def utrycatch2 = (x: Expr) =>
      consume(
        updateT,
        EUpdate(UpdateTryCatch(unitT, mk(updateT), var1, embed(x, optional(updateT)))),
      )
    def structUpd1 = (x: Expr) => consume(structT, EStructUpd(field, embed(x, structT), mk(unitT)))
    def structUpd2 = (x: Expr) => consume(structT, EStructUpd(field, mk(structT), x))
    def caseScrut = (x: Expr) => {
      ECase(
        embed(x, listT),
        ImmArray(
          CaseAlt(CPNil, theExp),
          CaseAlt(CPCons(var1, var2), theExp),
        ),
      )
    }
    def caseAlt1 = (x: Expr) => {
      ECase(
        mk(listT),
        ImmArray(
          CaseAlt(CPNil, x),
          CaseAlt(CPCons(var1, var2), theExp),
        ),
      )
    }
    def caseAlt2 = (x: Expr) => {
      ECase(
        mk(listT),
        ImmArray(
          CaseAlt(CPNil, theExp),
          CaseAlt(CPCons(var1, var2), x),
        ),
      )
    }
    def let1 = (x: Expr) => ELet(Binding(None, theType, x), theExp)
    def let2 = (x: Expr) => ELet(Binding(None, theType, theExp), x)

    // We don't test any recursion points for which the compiler requires access to a package
    // signature containing info for the type constructor.

    // This is the code under test...
    def typecheck(expr: Expr): Option[ValidationError] = {
      val langVersion: LanguageVersion = LanguageVersion.default
      val signatures: PartialFunction[PackageId, PackageSignature] = Map.empty
      val pkgInterface = new PackageInterface(signatures)
      val ctx: Context = Context.None
      val env = Typing.Env(langVersion, pkgInterface, ctx)
      try {
        val _: Type = env.typeOfTopExpr(expr)
        None
      } catch {
        case e: ValidationError => Some(e)
      }
    }

    // TODO https://github.com/digital-asset/daml/issues/13410
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
        ("uembed", uembed),
        ("utrycatch1", utrycatch1),
        ("utrycatch2", utrycatch2),
        ("structUpd1", structUpd1),
        ("structUpd2", structUpd2),
        ("caseScrut", caseScrut),
        ("caseAlt1", caseAlt1),
        ("caseAlt2", caseAlt2),
        ("let1", let1),
        ("let2", let2),
      )
    }

    {
      val depth = 10000 // big enough to demonstrate stack-safety
      s"typing, (LARGE) depth = $depth" - {
        forEvery(testCases) { (name: String, recursionPoint: Expr => Expr) =>
          name in {
            // ensure examples can be typechecked and are well-typed
            typecheck(makeAst(depth, theExp, recursionPoint)) shouldBe None
          }
        }
      }
    }
  }
}
