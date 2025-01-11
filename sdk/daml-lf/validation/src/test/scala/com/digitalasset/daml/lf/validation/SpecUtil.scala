// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.language.Ast.{Expr, Kind, Type}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import org.scalactic.Equality

private[validation] object SpecUtil {

  implicit val alphaEquivalence: Equality[Type] =
    (leftType: Type, right: Any) =>
      right match {
        case rightType: Type => AlphaEquiv.alphaEquiv(leftType, rightType)
        case _ => false
      }

  private val r = Map(
    'α' -> "alpha",
    'σ' -> "sigma",
    'τ' -> "tau",
    '⋆' -> "*",
    'Λ' -> """/\""",
    'λ' -> """\""",
    '∀' -> "forall",
    '→' -> "->",
    '←' -> "<-",
    '₁' -> "_1",
    '₂' -> "_2",
    '₃' -> "_3",
    '₄' -> "_4",
    'ᵢ' -> "_i",
    '⟨' -> "<",
    '⟩' -> ">",
    '⸨' -> "( loc(actual, test, 0, 0, 0, 0)( ",
    '⸩' -> " ))",
  )

  implicit class SyntaxHelper2(val sc: StringContext) extends AnyVal {
    def K(args: Any*): Kind =
      k"${replace(StringContext.standardInterpolator(identity, args, sc.parts))}"
    def T[P](args: Any*)(implicit parserParameters: ParserParameters[P]): Type =
      t"${replace(StringContext.standardInterpolator(identity, args, sc.parts))}"
    def E[P](args: Any*)(implicit parserParameters: ParserParameters[P]): Expr =
      e"${replace(StringContext.standardInterpolator(identity, args, sc.parts))}"

    def replace(s: String): String = {
      val b = new StringBuilder()
      for (c <- s) r.get(c).fold(b += c)(b ++= _)
      b.mkString
    }
  }
}
