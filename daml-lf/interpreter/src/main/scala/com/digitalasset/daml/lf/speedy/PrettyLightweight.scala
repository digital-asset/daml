// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util
import scala.collection.JavaConverters._

import com.daml.lf.speedy.Speedy._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._

object PrettyLightweight { // lightweight pretty printer for CEK machine states

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

  def ppKont(k: Kont): String = k match {
    case KFinished => "KFinished"
    case _: KArg => "KArg"
    case _: KFun => "KFun"
    case _: KBuiltin => "KBuiltin"
    case _: KPap => "KPap"
    case _: KPushTo => "KPushTo"
    case _: KCacheVal => "KCacheVal"
    case _: KLocation => "KLocation"
    case _: KCatch => "KCatch"
    case _: KLabelClosure => "KLabelClosure"
    case _: KLeaveClosure => "KLeaveClosure"
  }

  def pp(v: SELoc) = v match {
    case SELocS(n) => s"S#$n"
    case SELocA(n) => s"A#$n"
    case SELocF(n) => s"F#$n"
  }

  def pp(x: SDefinitionRef): String = {
    s"${x.ref.qualifiedName.name}"
  }

  def pp(e: SExpr): String =
    if (e == null) { "<null-expr>" } else
      e match {
        case SEValue(v) => s"(VALUE)${pp(v)}"
        case SEVar(n) => s"D#$n" //dont expect these at runtime
        case loc: SELoc => pp(loc)
        case SEAppGeneral(func, args) => s"@E(${pp(func)},${commas(args.map(pp))})"
        case SEAppAtomicGeneral(func, args) => s"@A(${pp(func)},${commas(args.map(pp))})"
        case SEAppAtomicSaturatedBuiltin(b, args) =>
          s"@B(${pp(SEBuiltin(b))},${commas(args.map(pp))})"
        case SEMakeClo(fvs, arity, body) => s"[${commas(fvs.map(pp))}]\\$arity.${pp(body)}"
        case SEBuiltin(b) => s"(BUILTIN)$b"
        case SEVal(ref) => s"(DEF)${pp(ref)}"
        case SELocation(_, exp) => s"LOC(${pp(exp)})"
        case SELet(rhss, body) => s"letG (${commas(rhss.map(pp))}) in ${pp(body)}"
        case SELet1General(rhs, body) => s"let ${pp(rhs)} in ${pp(body)}"
        case SELet1Builtin(builtin, args, body) =>
          s"letB (${pp(SEBuiltin(builtin))},${commas(args.map(pp))}) in ${pp(body)}"
        case SECaseAtomic(scrut, _) => s"case(atomic) ${pp(scrut)} of..."
        case SECase(scrut, _) => s"case ${pp(scrut)} of..."
        case SEBuiltinRecursiveDefinition(ref) => s"<SEBuiltinRecursiveDefinition:$ref>"
        case SECatch(_, _, _) => "<SECatch...>" //not seen one yet
        case SEAbs(_, _) => "<SEAbs...>" // will never get these on a running machine
        case SELabelClosure(_, _) => "<SELabelClosure...>"
        case SEImportValue(_) => "<SEImportValue...>"
        case SEWronglyTypeContractId(_, _, _) => "<SEWronglyTypeContractId...>"
      }

  def pp(v: SValue): String = v match {
    case SInt64(n) => s"$n"
    case SBool(b) => s"$b"
    case SPAP(_, args, arity) => s"PAP(${args.size}/$arity)"
    case SToken => "SToken"
    case SText(s) => s"'$s'"
    case SParty(_) => "<SParty>"
    case SStruct(_, _) => "<SStruct...>"
    case SUnit => "SUnit"
    case SList(_) => "SList"
    case _ => "<Unknown-value>" // TODO: complete cases
  }

  def pp(prim: Prim): String = prim match {
    case PBuiltin(b) => s"$b"
    case PClosure(_, expr, fvs) =>
      s"clo[${commas(fvs.map(pp))}]:${pp(expr)}"
  }

  def commas(xs: Seq[String]): String = xs.mkString(",")

}
