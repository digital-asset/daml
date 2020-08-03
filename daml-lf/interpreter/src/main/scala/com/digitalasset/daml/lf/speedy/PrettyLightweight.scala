// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util
import scala.collection.JavaConverters._

import com.daml.lf.speedy.Speedy._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._

private[speedy] object PrettyLightweight { // lightweight pretty printer for CEK machine states

  def ppMachine(m: Machine): String = {
    s"${ppEnv(m.env)} -- ${ppCtrl(m.ctrl, m.returnValue)} -- ${ppKontStack(m.kontStack)}"
  }

  def ppCtrl(e: SExpr, v: SValue): String =
    if (v != null) {
      s"V-${pp(v)}"
    } else {
      s"E-${pp(e)}"
    }

  def ppEnv(env: Env): String = {
    s"#${env.size()}={${commas(env.asScala.map(pp))}}"
  }

  def ppKontStack(ks: util.ArrayList[Kont]): String = {
    s"[${ppKont(ks.get(ks.size - 1))}... #${ks.size()}]" // head kont & size
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

  def pp(e: SExpr): String = e match {
    case SEValue(v) => s"(VALUE)${pp(v)}"
    case SEVar(n) => s"D#$n" //dont expect these at runtime
    case loc: SELoc => pp(loc)
    case SEAppGeneral(func, args) => s"@E(${pp(func)},${commas(args.map(pp))})"
    case SEAppAtomicFun(func, args) => s"@A(${pp(func)},${commas(args.map(pp))})"
    case SEAppSaturatedBuiltinFun(builtin, args) => s"@B($builtin,${commas(args.map(pp))})"
    case SEMakeClo(fvs, arity, body) => s"[${commas(fvs.map(pp))}]\\$arity.${pp(body)}"
    case SEBuiltin(b) => s"(BUILTIN)$b"
    case SEVal(ref) => s"(DEF)${pp(ref)}"
    case SELocation(_, exp) => s"LOC(${pp(exp)})"
    case SELet(rhss, body) => s"let (${commas(rhss.map(pp))}) in ${pp(body)}"
    case SECase(scrut, _) => s"case ${pp(scrut)} of..."
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

  def commas(xs: Seq[String]): String = xs.mkString(",")

}
