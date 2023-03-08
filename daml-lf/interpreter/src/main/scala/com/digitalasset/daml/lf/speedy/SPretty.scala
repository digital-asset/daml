// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import org.typelevel.paiges.Doc
import org.typelevel.paiges.Doc.{line, str, char, text, comma, space, intercalate}

import com.daml.lf.speedy.{SBuiltin => B}
import com.daml.lf.speedy.{SExpr => S}
import com.daml.lf.speedy.{SExpr0 => S0}
import com.daml.lf.speedy.{SValue => V}

import com.daml.lf.language.Ast

// Pretty-printer for speedy expressions...
private[lf] object SPretty {

  def pp0(exp: S0.SExpr): String = {
    docExp0(lev = 0)(exp).render(1)
  }

  private def docExp0(lev: Int)(exp: S0.SExpr): Doc = {

    exp match {
      case S0.SEVarLevel(n) =>
        text(s"x$n")

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
          (0 until arity).map { i => text(s"x${lev + i}") },
        ) + char(')') + text("->") / docExp0(lev + arity)(body).indent(2)

      case S0.SEApp(fun, args) =>
        docExp0(lev)(fun) + char('@') + docExp0List(lev)(args)

      case S0.SECase(scrut, alts) =>
        (text("case") + space + docExp0(lev)(scrut) + space + text("of")) / docCaseAlt0List(lev)(
          alts
        ).indent(2)

      case S0.SELet(boundS0, body) =>
        def loop(lev: Int)(bounds: List[S0.SExpr]): Doc = {
          bounds match {
            case Nil => docExp0(lev)(body)
            case boundExp :: bounds =>
              text(s"let x$lev = ") + docExp0(lev)(boundExp) / loop(lev + 1)(bounds)
          }
        }
        loop(lev)(boundS0).indent(2)

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

  private def docCaseAlt0List(lev: Int)(alts: List[S0.SCaseAlt]): Doc = {
    intercalate(line, alts.map(docCaseAlt0(lev)))
  }

  private def docCaseAlt0(lev: Int)(alt: S0.SCaseAlt): Doc = {
    alt match {
      case S0.SCaseAlt(pat, body) =>
        val n = pat.numArgs
        (docCasePat(lev)(pat) + space + text("->") + space) / docExp0(lev + n)(body).indent(2)
    }
  }
//  final case class SCaseAlt(pattern: SCasePat, body: SExpr)

  private def docCasePat(lev: Int)(pat: S.SCasePat): Doc = {
    val _ = lev
    pat match {
      case S.SCPPrimCon(Ast.PCTrue) => text("true")
      case S.SCPPrimCon(Ast.PCFalse) => text("false")
      case S.SCPNil => text("[]")
      case S.SCPCons => text(s"x$lev:x${lev + 1}")
      case S.SCPDefault => text("default")

      case _ =>
        ???
      // text(s"[PAT:$pat]")

      /*
        case SCPNil => (text("nil"), index)
        case SCPCons => (text("cons"), index + 2)
        case SCPVariant(_, v, _) =>
          (text("var") + char('(') + str(v) + char(')'), index + 1)
        case SCPEnum(_, v, _) =>
          (text("enum") + char('(') + str(v) + char(')'), index)
        case SCPPrimCon(pc) =>
          pc match {
            case PCTrue => (text("true"), index)
            case PCFalse => (text("false"), index)
            case PCUnit => (text("()"), index)
          }
        case SCPNone => (text("none"), index)
        case SCPSome => (text("some"), index + 1)
       */

    }

  }

  private def docExp0List(lev: Int)(exps: List[S0.SExpr]): Doc = {
    char('(') +
      intercalate(comma + space, exps.map(docExp0(lev))) +
      char(')')
  }

  private def docSDefinitionRef(sd: S.SDefinitionRef): Doc = {
    sd match {
      case S.LfDefRef(ref) =>
        text(ref.qualifiedName.name.toString)
      // text(ref.qualifiedName.toString) // more verbose
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
        // ??? // NICK
        text(s"[VALUE:$v)]")
    }
  }

  private def docSBuiltin(b: SBuiltin): Doc = {
    b match {
      case B.SBSubInt64 => text("(-)")
      case B.SBAddInt64 => text("(+)")
      case B.SBEqual => text("(==)")
      case B.SBCons => text("(:)")

      case B.SBRecCon(id, _) =>
        text(id.qualifiedName.name.toString)
      // text(id.qualifiedName.toString) //more verbose

      case _ =>
        // ??? // NICK
        text(s"[BUILTIN:$b]")
    }
  }

}
