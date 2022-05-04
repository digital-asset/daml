// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.language.Ast._

private[validation] object AlphaEquiv {

  def alphaEquiv(t1: Type, t2: Type): Boolean = Env().alphaEquiv(t1, t2)

  private case class Env(
      currentDepth: Int = 0,
      binderDepthLhs: Map[TypeVarName, Int] = Map.empty,
      binderDepthRhs: Map[TypeVarName, Int] = Map.empty,
  ) {

    def alphaEquiv(t1: Type, t2: Type): Boolean = (t1, t2) match {
      case (TVar(x1), TVar(x2)) =>
        binderDepthLhs.get(x1).toLeft(t1) == binderDepthRhs.get(x2).toLeft(t2)
      case (TNat(n1), TNat(n2)) => n1 == n2
      case (TTyCon(c1), TTyCon(c2)) => c1 == c2
      case (TApp(f1, a1), TApp(f2, a2)) => alphaEquiv(f1, f2) && alphaEquiv(a1, a2)
      case (TBuiltin(b1), TBuiltin(b2)) => b1 == b2
      case (TForall((varName1, kind1), b1), TForall((varName2, kind2), b2)) =>
        kind1 == kind2 &&
        Env(
          currentDepth + 1,
          binderDepthLhs + (varName1 -> currentDepth),
          binderDepthRhs + (varName2 -> currentDepth),
        ).alphaEquiv(b1, b2)
      case (TStruct(fs1), TStruct(fs2)) =>
        (fs1.names sameElements fs2.names) &&
        (fs1.values zip fs2.values).forall((alphaEquiv _).tupled)
      case (TSynApp(f, xs), TSynApp(g, ys)) =>
        // We treat type synonyms nominally here. If alpha equivalence
        // fails, we expand all of them and try again.
        f == g && xs.length == ys.length &&
        (xs.iterator zip ys.iterator).forall((alphaEquiv _).tupled)
      case _ => false
    }
  }
}
