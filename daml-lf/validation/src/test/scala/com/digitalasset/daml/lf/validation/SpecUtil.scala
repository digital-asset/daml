// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.language.Ast.{Expr, Kind, Package, Type}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.testing.parser.Implicits._
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
    '₁' → "_1",
    '₂' -> "_2",
    '₃' -> "_3",
    'ᵢ' -> "_i",
    '⟨' -> "<",
    '⟩' -> ">",
  )

  implicit class SyntaxHelper2(val sc: StringContext) extends AnyVal {
    def K(args: Any*): Kind = k"${replace(sc.standardInterpolator(identity, args))}"
    def T(args: Any*): Type = t"${replace(sc.standardInterpolator(identity, args))}"
    def E(args: Any*): Expr = e"${replace(sc.standardInterpolator(identity, args))}"

    def replace(s: String): String = {
      val b = new StringBuilder()
      for (c <- s) r.get(c).fold(b += c)(b ++= _)
      b.mkString
    }
  }

  implicit class PackageOps(val pkg: Package) extends AnyVal {
    def updateVersion(version: LanguageVersion) = {
      val modMap = pkg.modules.map {
        case (modName, mod) => modName -> mod.copy(languageVersion = version)
      }
      pkg.copy(modules = modMap)
    }
  }

}
