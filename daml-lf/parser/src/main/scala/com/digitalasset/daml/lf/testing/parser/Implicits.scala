// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing
package parser

import com.digitalasset.daml.lf.data.{Numeric, Ref}
import com.digitalasset.daml.lf.language.Ast.{Expr, Kind, Package, Type}

object Implicits {

  implicit val defaultParserParameters: ParserParameters[this.type] = ParserParameters(
    defaultPackageId,
    defaultLanguageVersion
  )

  implicit class SyntaxHelper(val sc: StringContext) extends AnyVal {
    def k(args: Any*): Kind = interpolate(KindParser.kind)(args)

    def t[P](args: Any*)(implicit parserParameters: ParserParameters[P]): Type =
      interpolate(new TypeParser[P](parserParameters).typ)(args)

    def e[P](args: Any*)(implicit parserParameters: ParserParameters[P]): Expr =
      interpolate(new ExprParser[P](parserParameters).expr)(args)

    def p[P](args: Any*)(implicit parserParameters: ParserParameters[P]): Package =
      interpolate(new ModParser[P](parserParameters).pkg)(args)

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def n(args: Any*): Ref.Name =
      Ref.Name.assertFromString(sc.standardInterpolator(identity, args.map(prettyPrint)))

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    private def interpolate[T](p: Parsers.Parser[T])(args: Seq[Any]): T =
      Parsers.parseAll(Parsers.phrase(p), sc.standardInterpolator(identity, args.map(prettyPrint)))
  }

  private def toString(x: BigDecimal) =
    Numeric.toUnscaledString(Numeric.assertFromUnscaledBigDecimal(x))

  private def prettyPrint(x: Any): String =
    x match {
      case d: BigDecimal => toString(d)
      case d: Float => toString(d.toDouble)
      case d: Double => toString(d)
      case other: Any => other.toString
    }
}
