// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.language.Ast._

import scala.annotation.tailrec

private[validation] object AlphaEquiv {

  def alphaEquiv(t1: Type, t2: Type): Boolean = alphaEquivList(List((Env(), t1, t2)))

  private case class Env(
      currentDepth: Int = 0,
      binderDepthLhs: Map[TypeVarName, Int] = Map.empty,
      binderDepthRhs: Map[TypeVarName, Int] = Map.empty,
  ) {
    def extend(varName1: TypeVarName, varName2: TypeVarName) = Env(
      currentDepth + 1,
      binderDepthLhs + (varName1 -> currentDepth),
      binderDepthRhs + (varName2 -> currentDepth),
    )
  }

  @tailrec
  private def alphaEquivList(trips: List[(Env, Type, Type)]): Boolean = trips match {
    case Nil => true
    case (env, t1, t2) :: trips =>
      (t1, t2) match {
        case (TVar(x1), TVar(x2)) =>
          env.binderDepthLhs.get(x1).toLeft(t1) == env.binderDepthRhs.get(x2).toLeft(t2) &&
          alphaEquivList(trips)
        case (TNat(n1), TNat(n2)) =>
          n1 == n2 && alphaEquivList(trips)
        case (TTyCon(c1), TTyCon(c2)) =>
          c1 == c2 && alphaEquivList(trips)
        case (TApp(f1, a1), TApp(f2, a2)) =>
          alphaEquivList((env, f1, f2) :: (env, a1, a2) :: trips)
        case (TBuiltin(b1), TBuiltin(b2)) =>
          b1 == b2 && alphaEquivList(trips)
        case (TForall((varName1, kind1), b1), TForall((varName2, kind2), b2)) =>
          kind1 == kind2 && {
            val envExtended = env.extend(varName1, varName2)
            alphaEquivList((envExtended, b1, b2) :: trips)
          }
        case (TStruct(fs1), TStruct(fs2)) =>
          (fs1.names sameElements fs2.names) && {
            val more = (fs1.values zip fs2.values).map { case (x1, x2) => (env, x1, x2) }
            alphaEquivList(more ++: trips)
          }
        case (TSynApp(f, xs), TSynApp(g, ys)) =>
          // We treat type synonyms nominally here. If alpha equivalence
          // fails, we expand all of them and try again.
          f == g && xs.length == ys.length && {
            val more = (xs.iterator zip ys.iterator).map { case (x1, x2) => (env, x1, x2) }
            alphaEquivList(more ++: trips)
          }
        case _ =>
          false
      }
  }
}
