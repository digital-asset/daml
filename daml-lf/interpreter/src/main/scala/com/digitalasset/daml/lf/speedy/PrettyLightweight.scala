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
    //s"{#${env.size()}}" //show just the env size
  }

  def ppKontStack(ks: util.ArrayList[Kont]): String = {
    //ks.asScala.reverse.map(ppKont).mkString(" -- ")
    s"[#${ks.size()}]" //show just the kont-stack depth
  }

  def ppKont(k: Kont): String = k match {
    case KArg(es, _, _) => s"KArg(${commas(es.map(pp))})"
    case KFun(prim, extendedArgs, arity) =>
      s"KFun(${pp(prim)}/$arity,[${commas(extendedArgs.asScala.map(pp))}])"
    case KPushTo(_, e, _, _) => s"KPushTo(_, ${pp(e)})"
    case KCacheVal(_, _) => "KCacheVal"
    case KLocation(_) => "KLocation"
    case KMatch(_, _, _) => "KMatch"
    case KCatch(_, _, _, _) => "KCatch"
    case KFinished => "KFinished"
    case KLabelClosure(_) => "KLabelClosure"
    case KLeaveClosure(_) => "KLeaveClosure"
  }

  def pp(v: SELoc) = v match {
    case SELocS(n) => s"S#$n"
    case SELocA(n) => s"A#$n"
    case SELocF(n) => s"F#$n"
  }

  def pp(e: SExpr): String = e match {
    case SEValue(v) => pp(v)
    case SEVar(n) => s"D#$n" //dont expect thee at runtime
    case loc: SELoc => pp(loc)
    //case SEApp(func, args) => s"@(${pp(func)},${commas(args.map(pp))})"
    case SEApp(_, _) => s"@(...)"
    //case SEMakeClo(fvs, arity, body) => s"[${commas(fvs.map(ppVarRef))}]lam/$arity->${pp(body)}"
    case SEMakeClo(fvs, arity, _) => s"[${commas(fvs.map(pp))}]lam/$arity->..."
    case SEBuiltin(b) => s"${b}"
    case SEVal(_) => "<SEVal...>"
    case SELocation(_, _) => "<SELocation...>"
    case SELet(_, _) => "<SELet...>"
    case SECase(_, _) => "<SECase...>"
    case SEBuiltinRecursiveDefinition(_) => "<SEBuiltinRecursiveDefinition...>"
    case SECatch(_, _, _) => "<SECatch...>" //not seen one yet
    case SEAbs(_, _) => "<SEAbs...>" // will never get these on a running machine
    case SELabelClosure(_, _) => "<SELabelClosure...>"
    case SEImportValue(_) => "<SEImportValue...>"
    case SEWronglyTypeContractId(_, _, _) => "<SEWronglyTypeContractId...>"
  }

  def pp(v: SValue): String = v match {
    case SInt64(n) => s"$n"
    case SPAP(prim, args, arity) =>
      s"PAP[${args.size}/$arity](${pp(prim)}(${commas(args.asScala.map(pp))})))"
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
