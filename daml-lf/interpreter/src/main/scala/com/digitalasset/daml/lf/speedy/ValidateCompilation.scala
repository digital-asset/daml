// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

/**  ValidateCompilation (Phase of the speedy compiler pipeline)
  *
  *  This phases validates the final compilation result -- SExpr
  */

import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SExpr._

import scala.annotation.tailrec

private[lf] object ValidateCompilation {

  case class CompilationError(error: String) extends RuntimeException(error, null, true, false)

  private[speedy] def validateCompilation(expr0: SExpr): SExpr = {

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

    def goBody(maxS: Int, maxA: Int, maxF: Int): SExpr => Unit = {

      def goLoc(loc: SELoc) = loc match {
        case SELocS(i) =>
          if (i < 1 || i > maxS)
            throw CompilationError(s"validate: SELocS: index $i out of range ($maxS..1)")
        case SELocA(i) =>
          if (i < 0 || i >= maxA)
            throw CompilationError(s"validate: SELocA: index $i out of range (0..$maxA-1)")
        case SELocF(i) =>
          if (i < 0 || i >= maxF)
            throw CompilationError(s"validate: SELocF: index $i out of range (0..$maxF-1)")
      }

      def go(expr: SExpr): Unit = expr match {
        case loc: SELoc => goLoc(loc)
        case _: SEVal => ()
        case _: SEBuiltin => ()
        case _: SEBuiltinRecursiveDefinition => ()
        case SEValue(v) => goV(v)
        case SEAppAtomicGeneral(fun, args) =>
          go(fun)
          args.foreach(go)
        case SEAppAtomicSaturatedBuiltin(_, args) =>
          args.foreach(go)
        case SEAppGeneral(fun, args) =>
          go(fun)
          args.foreach(go)
        case SEAppAtomicFun(fun, args) =>
          go(fun)
          args.foreach(go)
        case SEMakeClo(fvs, n, body) =>
          fvs.foreach(goLoc)
          goBody(0, n, fvs.length)(body)
        case SECaseAtomic(scrut, alts) =>
          go(scrut)
          alts.foreach { case SCaseAlt(pat, body) =>
            val n = pat.numArgs
            goBody(maxS + n, maxA, maxF)(body)
          }
        case _: SELet1General => goLets(maxS)(expr)
        case _: SELet1Builtin => goLets(maxS)(expr)
        case _: SELet1BuiltinArithmetic => goLets(maxS)(expr)
        case SELocation(_, body) =>
          go(body)
        case SELabelClosure(_, expr) =>
          go(expr)
        case SETryCatch(body, handler) =>
          go(body)
          goBody(maxS + 1, maxA, maxF)(handler)
        case SEScopeExercise(body) =>
          go(body)

        case _: SEDamlException | _: SEImportValue =>
          throw CompilationError(s"validate: unexpected $expr")
      }
      @tailrec
      def goLets(maxS: Int)(expr: SExpr): Unit = {
        def go = goBody(maxS, maxA, maxF)
        expr match {
          case SELet1General(rhs, body) =>
            go(rhs)
            goLets(maxS + 1)(body)
          case SELet1Builtin(_, args, body) =>
            args.foreach(go)
            goLets(maxS + 1)(body)
          case SELet1BuiltinArithmetic(_, args, body) =>
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
