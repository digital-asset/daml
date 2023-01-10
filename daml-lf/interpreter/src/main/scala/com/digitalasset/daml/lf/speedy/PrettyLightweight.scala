// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import scala.jdk.CollectionConverters._

import com.daml.lf.speedy.Speedy._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._

import scala.annotation.nowarn

private[speedy] object PrettyLightweight { // lightweight pretty printer for CEK machine states

  def ppMachine(m: Machine[_]): String = {
    s"[${m.currentEnvBase}] ${ppEnv(m.currentEnv)} -- ${ppCtrl(m.currentControl)} -- ${ppKontStack(m)}"
  }

  def ppCtrl(control: Control[_]): String =
    control match {
      case Control.WeAreUnset => "unset"
      case Control.Value(v) => s"V-${pp(v)}"
      case Control.Expression(e) => s"E-${pp(e)}"
      case Control.Question(_) => "question"
      case Control.Complete(_) => "complete"
      case Control.Error(_) => "error"
    }

  def ppEnv(env: Env): String = {
    s"#${env.size()}={${commas(env.asScala.map(pp))}}"
  }

  def ppKontStack(m: Machine[_]): String = {
    val depth = m.kontDepth()
    if (depth == 0) {
      s"[#0]"
    } else {
      s"[${ppKont(m.peekKontStackEnd())}... #${depth}]" // head kont & size
    }
  }

  def ppKont(k: Kont): String = k.getClass.getSimpleName

  def pp(v: SELoc) = v match {
    case SELocS(n) => s"S#$n"
    case SELocA(n) => s"A#$n"
    case SELocF(n) => s"F#$n"
  }

  def pp(x: SDefinitionRef): String = {
    s"${x.ref.qualifiedName.name}"
  }

  @nowarn("cat=deprecation&origin=com.daml.lf.speedy.SExpr.SEAppOnlyFunIsAtomic")
  def pp(e: SExpr): String = e match {
    case SEValue(v) => s"(VALUE)${pp(v)}"
    case loc: SELoc => pp(loc)
    case SEAppOnlyFunIsAtomic(func, args) => s"@N(${pp(func)},${commas(args.map(pp))})"
    case SEAppAtomicGeneral(func, args) => s"@A(${pp(func)},${commas(args.map(pp))})"
    case SEAppAtomicSaturatedBuiltin(b, args) => s"@B(${pp(SEBuiltin(b))},${commas(args.map(pp))})"
    case SEMakeClo(fvs, arity, body) => s"[${commas(fvs.map(pp))}]\\$arity.${pp(body)}"
    case SEBuiltin(b) => s"(BUILTIN)$b"
    case SEVal(ref) => s"(DEF)${pp(ref)}"
    case SELocation(_, exp) => s"LOC(${pp(exp)})"
    case SELet1General(rhs, body) => s"let ${pp(rhs)} in ${pp(body)}"
    case SELet1Builtin(builtin, args, body) =>
      s"letB (${pp(SEBuiltin(builtin))},${commas(args.map(pp))}) in ${pp(body)}"
    case SECaseAtomic(scrut, _) => s"case(atomic) ${pp(scrut)} of..."
    case _ => "<" + e.getClass.getSimpleName + "...>"
  }

  def pp(v: SValue): String = v match {
    case SInt64(n) => s"$n"
    case SBool(b) => s"$b"
    case SPAP(_, args, arity) => s"PAP(${args.size}/$arity)"
    case SText(s) => s"'$s'"
    case _ => "<" + v.getClass.getSimpleName + "...>"
  }

  def pp(prim: Prim): String = prim match {
    case PBuiltin(b) => s"$b"
    case PClosure(_, expr, fvs) =>
      s"clo[${commas(fvs.map(pp))}]:${pp(expr)}"
  }

  def commas(xs: collection.Seq[String]): String = xs.mkString(",")

}
