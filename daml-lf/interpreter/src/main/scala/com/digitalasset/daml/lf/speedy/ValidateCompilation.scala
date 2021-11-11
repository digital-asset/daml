// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

/**  ValidateCompilation (Phase of the speedy compiler pipeline)
  *
  *  This phases validates the final compilation result -- SExpr
  */

import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{SExpr => t}

import scala.annotation.tailrec

private[lf] object ValidateCompilation {

  case class CompilationError(error: String) extends RuntimeException(error, null, true, false)

  private[speedy] def validateCompilation(expr0: t.SExpr): t.SExpr = {

    def goV(v: SValue): Unit =
      v match {
        case _: SPrimLit | STNat(_) | STypeRep(_) =>
        case SList(a) => a.iterator.foreach(goV)
        case SOptional(x) => x.foreach(goV)
        case SMap(_, entries) =>
          entries.foreach { case (k, v) =>
            goV(k)
            goV(v)
          }
        case SRecord(_, _, args) => args.forEach(goV)
        case SVariant(_, _, _, value) => goV(value)
        case SEnum(_, _, _) => ()
        case SAny(_, v) => goV(v)
        case _: SPAP | SToken | SStruct(_, _) =>
          throw CompilationError("validate: unexpected s.SEValue")
      }

    def goBody(maxS: Int, maxA: Int, maxF: Int): t.SExpr => Unit = {

      def goLoc(loc: t.SELoc) = loc match {
        case t.SELocS(i) =>
          if (i < 1 || i > maxS)
            throw CompilationError(s"validate: SELocS: index $i out of range ($maxS..1)")
        case t.SELocA(i) =>
          if (i < 0 || i >= maxA)
            throw CompilationError(s"validate: SELocA: index $i out of range (0..$maxA-1)")
        case t.SELocF(i) =>
          if (i < 0 || i >= maxF)
            throw CompilationError(s"validate: SELocF: index $i out of range (0..$maxF-1)")
      }

      def go(expr: t.SExpr): Unit = expr match {
        case loc: t.SELoc => goLoc(loc)
        case _: t.SEVal => ()
        case _: t.SEBuiltin => ()
        case _: t.SEBuiltinRecursiveDefinition => ()
        case t.SEValue(v) => goV(v)
        case t.SEAppAtomicGeneral(fun, args) =>
          go(fun)
          args.foreach(go)
        case t.SEAppAtomicSaturatedBuiltin(_, args) =>
          args.foreach(go)
        case t.SEAppGeneral(fun, args) =>
          go(fun)
          args.foreach(go)
        case t.SEAppAtomicFun(fun, args) =>
          go(fun)
          args.foreach(go)
        case t.SEMakeClo(fvs, n, body) =>
          fvs.foreach(goLoc)
          goBody(0, n, fvs.length)(body)
        case t.SECaseAtomic(scrut, alts) =>
          go(scrut)
          alts.foreach { case t.SCaseAlt(pat, body) =>
            val n = pat.numArgs
            goBody(maxS + n, maxA, maxF)(body)
          }
        case _: t.SELet1General => goLets(maxS)(expr)
        case _: t.SELet1Builtin => goLets(maxS)(expr)
        case _: t.SELet1BuiltinArithmetic => goLets(maxS)(expr)
        case t.SELocation(_, body) =>
          go(body)
        case t.SELabelClosure(_, expr) =>
          go(expr)
        case t.SETryCatch(body, handler) =>
          go(body)
          goBody(maxS + 1, maxA, maxF)(handler)
        case t.SEScopeExercise(body) =>
          go(body)

        case _: t.SEDamlException | _: t.SEImportValue =>
          throw CompilationError(s"validate: unexpected $expr")
      }
      @tailrec
      def goLets(maxS: Int)(expr: t.SExpr): Unit = {
        def go = goBody(maxS, maxA, maxF)
        expr match {
          case t.SELet1General(rhs, body) =>
            go(rhs)
            goLets(maxS + 1)(body)
          case t.SELet1Builtin(_, args, body) =>
            args.foreach(go)
            goLets(maxS + 1)(body)
          case t.SELet1BuiltinArithmetic(_, args, body) =>
            args.foreach(go)
            goLets(maxS + 1)(body)
          case expr =>
            go(expr)
        }
      }
      go
    }
    goBody(0, 0, 0)(expr0)
    expr0
  }

}
