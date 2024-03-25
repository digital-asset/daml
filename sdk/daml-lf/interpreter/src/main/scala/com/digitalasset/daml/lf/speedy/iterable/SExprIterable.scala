// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy.iterable

import com.daml.lf.speedy.{SExpr, SValue}
import com.daml.lf.speedy.SExpr.SExpr
import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

// Iterates only over immediate children similar to Haskell’s
// uniplate.
@nowarn("cat=deprecation&origin=com.daml.lf.speedy.SExpr.SEAppOnlyFunIsAtomic")
private[speedy] object SExprIterable {
  that =>
  private[iterable] def iterator(e: SExpr): Iterator[SExpr] = e match {
    case SExpr.SEVal(_) => Iterator.empty
    case SExpr.SEAppOnlyFunIsAtomic(fun, args) => Iterator(fun) ++ args.iterator
    case SExpr.SEAppAtomicGeneral(fun, args) => Iterator(fun) ++ args.iterator
    case SExpr.SEAppAtomicSaturatedBuiltin(_, args) => args.iterator
    case SExpr.SEMakeClo(_, _, body) => Iterator(body)
    case SExpr.SECaseAtomic(scrut, alts) => Iterator(scrut) ++ alts.iterator.map(_.body)
    case SExpr.SELet1General(rhs, body) => Iterator(rhs, body)
    case SExpr.SELet1Builtin(_, args, body) => args.iterator ++ Iterator(body)
    case SExpr.SELet1BuiltinArithmetic(_, args, body) => args.iterator ++ Iterator(body)
    case SExpr.SELocation(_, expr) => Iterator(expr)
    case SExpr.SELabelClosure(_, expr) => Iterator(expr)
    case _: SExpr.SEImportValue => Iterator.empty
    case SExpr.SETryCatch(body, handler) => Iterator(body, handler)
    case SExpr.SEScopeExercise(body) => Iterator(body)
    case SExpr.SEPreventCatch(body) => Iterator(body)
    case SExpr.SEBuiltin(_) => Iterator.empty
    case SExpr.SELocA(_) => Iterator.empty
    case SExpr.SELocS(_) => Iterator.empty
    case SExpr.SEValue(v) => iterator(v)
    case SExpr.SELocF(_) => Iterator.empty
    case SExpr.SEDelayedCrash(_, _) => Iterator.empty
  }
  private[this] def iterator(v: SValue): Iterator[SExpr] = v match {
    case SValue.SPAP(prim, actuals, _) =>
      iterator(prim) ++ actuals.asScala.iterator.flatMap(iterator(_))
    case _: SValue.SPrimLit | SValue.STypeRep(_) | SValue.SToken | SValue.SAny(_, _) |
        SValue.SEnum(_, _, _) | SValue.SMap(_, _) | SValue.SList(_) | SValue.SOptional(_) |
        _: SValue.SRecordRep | SValue.SStruct(_, _) | SValue.SVariant(_, _, _, _) =>
      SValueIterable.iterator(v).flatMap(iterator(_))
  }
  private[this] def iterator(v: SValue.Prim): Iterator[SExpr] = v match {
    case SValue.PBuiltin(_) => Iterator.empty
    case SValue.PClosure(_, expr, frame) => Iterator(expr) ++ frame.iterator.flatMap(iterator(_))
  }

  def apply(v: SValue): Iterable[SExpr] =
    new Iterable[SExpr] {
      override def iterator = that.iterator(v)
    }

  def apply(v: SExpr): Iterable[SExpr] =
    new Iterable[SExpr] {
      override def iterator = that.iterator(v)
    }
}
