// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import org.typelevel.paiges.Doc
import org.typelevel.paiges.Doc.{line, str, char, text, comma, space, intercalate}

import com.daml.lf.speedy.{SBuiltin => B}
import com.daml.lf.speedy.{SExpr => S}
import com.daml.lf.speedy.{SExpr0 => S0}
import com.daml.lf.speedy.{SExpr1 => S1}
import com.daml.lf.speedy.{SExpr => S2}
import com.daml.lf.speedy.{SValue => V}

import com.daml.lf.language.Ast

import scala.annotation.nowarn

// Pretty-printer for speedy expression forms: SExpr0, SExpr1, SExpr

private[lf] object SPretty {

  // Entry points...

  def pp0(exp: S0.SExpr): String = {
    docExp0(lev = 0)(exp).indent(4).render(1)
  }

  def pp1(exp: S1.SExpr): String = {
    docExp1(lev = 0)(exp).indent(4).render(1)
  }

  def pp2(exp: S2.SExpr): String = {
    docExp2(lev = 0)(exp).indent(4).render(1)
  }

  // ----------------------------------------------------------------------
  // SExpr0 (After Phase1)...

  private def docExp0(lev: Int)(exp: S0.SExpr): Doc = {

    val varNamePrefix = "x"

    exp match {
      case S0.SEVarLevel(n) =>
        text(s"$varNamePrefix$n")

      case S0.SEVal(sd) =>
        docSDefinitionRef(sd)

      case S0.SEBuiltin(b) =>
        docSBuiltin(b)

      case S0.SEValue(v) =>
        docSValue(v)

      case S0.SELocation(loc, body) =>
        val _ = (loc, body)
        ???

      case S0.SEAbs(arity, body) =>
        char('\\') + char('(') + intercalate(
          comma,
          (0 until arity).map { i => text(s"$varNamePrefix${lev + i}") }, // NOTE: lev
        ) + char(')') + text("->") / docExp0(lev + arity)(body).indent(2) // NOTE: lev

      case S0.SEApp(fun, args) =>
        docExp0(lev)(fun) + text(" @") + docExp0List(lev)(args)

      case S0.SECase(scrut, alts) =>
        (text("case") + space + docExp0(lev)(scrut) + space + text("of")) / intercalate(
          line,
          alts.map(docCaseAlt0(varNamePrefix)(lev)),
        ).indent(2)

      case S0.SELet(boundS0, body) =>
        def loop(lev: Int)(bounds: List[S0.SExpr]): Doc = {
          bounds match {
            case Nil => docExp0(lev)(body)
            case boundExp :: bounds =>
              text(s"let $varNamePrefix$lev = ") + docExp0(lev)(boundExp) / loop(lev + 1)(bounds)
          }
        }
        loop(lev)(boundS0) // .indent(2)

      case S0.SETryCatch(body, handler) =>
        val _ = (body, handler)
        ???

      case S0.SEScopeExercise(body) =>
        val _ = body
        ???

      case S0.SEPreventCatch(body) =>
        val _ = body
        ???

      case S0.SELabelClosure(label, expr) =>
        val _ = (label, expr)
        ???
    }
  }

  private def docExp0List(lev: Int)(exps: List[S0.SExpr]): Doc = {
    char('(') +
      intercalate(comma + space, exps.map(docExp0(lev))) +
      char(')')
  }

  private def docCaseAlt0(prefix: String)(lev: Int)(alt: S0.SCaseAlt): Doc = {
    alt match {
      case S0.SCaseAlt(pat, body) =>
        val n = pat.numArgs
        (docCasePat(prefix, lev)(pat) + space + text("->")) / docExp0(lev + n)(body).indent(2)
    }
  }

  // ----------------------------------------------------------------------
  // SExpr1 (After CC)...

  private def docLoc1(loc: S1.SELoc): Doc = {
    loc match {
      case S1.SELocAbsoluteS(n) => text(s"stack$n")
      case S1.SELocA(n) => text(s"arg$n")
      case S1.SELocF(n) => text(s"free$n")
    }
  }

  private def docExp1(lev: Int)(exp: S1.SExpr): Doc = {

    exp match {

      case loc: S1.SELoc => docLoc1(loc)

      case S1.SEVal(sd) =>
        docSDefinitionRef(sd)

      case S1.SEBuiltin(b) =>
        docSBuiltin(b)

      case S1.SEValue(v) =>
        docSValue(v)

      case S1.SELocation(loc, body) =>
        val _ = (loc, body)
        ???

      case S1.SEMakeClo(fvs, arity, body) =>
        val _ = (fvs, arity, body)
        text("CLOSE{") + intercalate(
          comma,
          fvs.zipWithIndex.map { case (loc, i) =>
            text(s"free$i=") + docLoc1(loc)
          },
        ) + char('}') +
          char('\\') + char('(') + intercalate(
            comma,
            (0 until arity).map { i => text(s"arg$i") }, // NOTE: ignore lev here
          ) + char(')') + text("->") / docExp1(0)(body).indent(2) // NOTE: and here

      case S1.SEApp(fun, args) =>
        docExp1(lev)(fun) + text(" @") + docExp1List(lev)(args)

      case S1.SECase(scrut, alts) =>
        (text("case") + space + docExp1(lev)(scrut) + space + text("of")) / intercalate(
          line,
          alts.map(docCaseAlt1(lev)),
        ).indent(2)

      case S1.SELet(bounds0, body) =>
        def loop(lev: Int)(bounds: List[S1.SExpr]): Doc = {
          bounds match {
            case Nil => docExp1(lev)(body)
            case boundExp :: bounds =>
              text(s"let stack$lev = ") + docExp1(lev)(boundExp) / loop(lev + 1)(bounds)
          }
        }
        loop(lev)(bounds0) // .indent(2)

      case S1.SELet1General(rhs, body) =>
        val _ = (rhs, body)
        ???

      case S1.SETryCatch(body, handler) =>
        val _ = (body, handler)
        ???

      case S1.SEScopeExercise(body) =>
        val _ = body
        ???

      case S1.SEPreventCatch(body) =>
        val _ = body
        ???

      case S1.SELabelClosure(label, expr) =>
        val _ = (label, expr)
        ???
    }
  }

  private def docExp1List(lev: Int)(exps: List[S1.SExpr]): Doc = {
    char('(') +
      intercalate(comma + space, exps.map(docExp1(lev))) +
      char(')')
  }

  private def docCaseAlt1(lev: Int)(alt: S1.SCaseAlt): Doc = {
    alt match {
      case S1.SCaseAlt(pat, body) =>
        val n = pat.numArgs
        (docCasePat("stack", lev)(pat) + space + text("->")) / docExp1(lev + n)(body)
          .indent(2)
    }
  }

  // ----------------------------------------------------------------------
  // SExpr (After ANF)...

  private def docLoc2(lev: Int)(loc: S2.SELoc): Doc = {
    loc match {
      // convert relative stack offsets to absolute stack var names
      case S2.SELocS(rel) => text(s"stack${lev - rel}")
      case S2.SELocA(n) => text(s"arg$n")
      case S2.SELocF(n) => text(s"free$n")
    }
  }

  @nowarn("cat=deprecation&origin=com.daml.lf.speedy.SExpr.SEAppOnlyFunIsAtomic")
  private def docExp2(lev: Int)(exp: S2.SExpr): Doc = {

    exp match {

      case loc: S2.SELoc => docLoc2(lev)(loc)

      case S2.SEVal(sd) =>
        docSDefinitionRef(sd)

      case S2.SEBuiltin(b) =>
        docSBuiltin(b)

      case S2.SEValue(v) =>
        docSValue(v)

      case S2.SELocation(loc, body) =>
        val _ = (loc, body)
        ???

      case S2.SEMakeClo(fvs, arity, body) =>
        val _ = (fvs, arity, body)
        text("CLOSE{") + intercalate(
          comma,
          fvs.zipWithIndex.map { case (loc, i) =>
            text(s"free$i=") + docLoc2(lev)(loc)
          },
        ) + char('}') +
          char('\\') + char('(') + intercalate(
            comma,
            (0 until arity).map { i => text(s"arg$i") }, // NOTE: ignore lev here
          ) + char(')') + text("->") / docExp2(0)(body).indent(2) // NOTE: and here

      case S2.SEAppAtomicGeneral(fun, args) =>
        docExp2(lev)(fun) + text(" @A") + docExp2List(lev)(args.toList)

      case S2.SEAppAtomicSaturatedBuiltin(builtin, args) =>
        docSBuiltin(builtin) + text(" @B") + docExp2List(lev)(args.toList)

      case S2.SEAppOnlyFunIsAtomic(fun, args) =>
        docExp2(lev)(fun) + text(" @G") + docExp2List(lev)(args.toList)

      case S2.SECaseAtomic(scrut, alts) =>
        (text("case") + space + docExp2(lev)(scrut) + space + text("of")) / intercalate(
          line,
          alts.map(docCaseAlt2(lev)),
        ).indent(2)

      case S2.SELet1General(rhs, body) =>
        text(s"let stack$lev = ") + docExp2(lev)(rhs) / docExp2(lev + 1)(body)

      case S2.SELet1Builtin(builtin, args, body) =>
        text(s"let stack$lev = ") + docSBuiltin(builtin) + text(" @B") + docExp2List(lev)(
          args.toList
        ) / docExp2(lev + 1)(body)

      case S2.SELet1BuiltinArithmetic(builtin, args, body) =>
        text(s"let stack$lev = ") + docSBuiltin(builtin) + text(" @B") + docExp2List(lev)(
          args.toList
        ) / docExp2(lev + 1)(body)

      case S2.SETryCatch(body, handler) =>
        val _ = (body, handler)
        ???

      case S2.SEScopeExercise(body) =>
        val _ = body
        ???

      case S2.SEPreventCatch(body) =>
        val _ = body
        ???

      case S2.SELabelClosure(label, expr) =>
        val _ = (label, expr)
        ???

      case S2.SEImportValue(_, _) =>
        ???
    }
  }

  private def docExp2List(lev: Int)(exps: List[S2.SExpr]): Doc = {
    char('(') +
      intercalate(comma + space, exps.map(docExp2(lev))) +
      char(')')
  }

  private def docCaseAlt2(lev: Int)(alt: S2.SCaseAlt): Doc = {
    alt match {
      case S2.SCaseAlt(pat, body) =>
        val n = pat.numArgs
        (docCasePat("stack", lev)(pat) + space + text("->")) / docExp2(lev + n)(body)
          .indent(2)
    }
  }

  // ----------------------------------------------------------------------
  // common: def-ref, value, builtin, pattern

  private def docSDefinitionRef(sd: S.SDefinitionRef): Doc = {
    sd match {
      case S.LfDefRef(ref) =>
        text(ref.qualifiedName.toString)
      case _ =>
        ???
    }
  }

  private def docSValue(v: SValue): Doc = {
    v match {
      case V.SBool(true) => text("true")
      case V.SBool(false) => text("false")
      case V.SInt64(n) => str(n)
      case V.SList(fs) =>
        text("[") +
          intercalate(comma + space, fs.iterator.toList.map(docSValue(_))) +
          text("]")
      case V.SText(s) =>
        char('"') + text(s) + char('"')
      case _ =>
        text(s"[VALUE:$v)]")
    }
  }

  private def docSBuiltin(b: SBuiltin): Doc = {
    b match {
      case B.SBRecCon(id, _) => text(id.qualifiedName.toString)
      case B.SBRecProj(id, field) => text(s"[SBRecProj(${id.qualifiedName}, $field]")
      case _ => text(s"$b")
    }
  }

  private def docCasePat(prefix: String, lev: Int)(pat: S.SCasePat): Doc = {
    pat match {
      case S.SCPPrimCon(Ast.PCTrue) => text("True")
      case S.SCPPrimCon(Ast.PCFalse) => text("False")
      case S.SCPNil => text("Nil")
      case S.SCPCons => text(s"Cons($prefix$lev,$prefix${lev + 1})")
      case S.SCPDefault => text("default")
      case _ => text(s"PAT:$pat")
    }
  }
}
