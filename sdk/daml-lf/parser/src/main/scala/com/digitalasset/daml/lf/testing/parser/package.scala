// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing

import com.daml.lf.language.Ast.{Expr, Kind, Module, Type}

/** The LF Parser library can be used to write Daml-LF Ast using a
  * human-friendly syntax.
  *
  * It is designed for testing only and provided without any guarantee.
  * In particular future version may introduce breaking change without notice.
  */
package object parser {

  private def safeParse[T](p: Parsers.Parser[T], s: String): Either[String, T] =
    try {
      Right(Parsers.parseAll(p, s))
    } catch {
      case e: ParserError =>
        Left(e.description)
    }

  def parseKind(s: String): Either[String, Kind] =
    safeParse(KindParser.kind, s)
  def parseType[P](s: String)(implicit
      parserParameters: ParserParameters[P]
  ): Either[String, Type] =
    safeParse(new TypeParser[P](parserParameters).typ, s)
  def parseExpr[P](s: String)(implicit
      parserParameters: ParserParameters[P]
  ): Either[String, Expr] =
    safeParse(new ExprParser[P](parserParameters).expr, s)
  def parseExprs[P](s: String)(implicit
      parserParameters: ParserParameters[P]
  ): Either[String, List[Expr]] =
    safeParse(new ExprParser[P](parserParameters).exprs, s)
  def parseModules[P](s: String)(implicit
      parserParameters: ParserParameters[P]
  ): Either[String, List[Module]] =
    safeParse(Parsers.rep(new ModParser[P](parserParameters).mod), s)

}
