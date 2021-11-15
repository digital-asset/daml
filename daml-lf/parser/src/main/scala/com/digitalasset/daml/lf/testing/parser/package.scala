// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast.{Expr, Kind, Module, Type}
import com.daml.lf.language.{FixNat, LanguageVersion}

package object parser {

  val defaultPackageId = Ref.PackageId.assertFromString("-pkgId-")
  val defaultLanguageVersion = LanguageVersion.default

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
    safeParse(new TypeParser[P](parserParameters).typ, s).map(FixNat.processType)
  def parseExpr[P](s: String)(implicit
      parserParameters: ParserParameters[P]
  ): Either[String, Expr] =
    safeParse(new ExprParser[P](parserParameters).expr, s).map(FixNat.processExpr)
  def parseExprs[P](s: String)(implicit
      parserParameters: ParserParameters[P]
  ): Either[String, List[Expr]] =
    safeParse(new ExprParser[P](parserParameters).exprs, s).map(_.map(FixNat.processExpr))
  def parseModules[P](s: String)(implicit
      parserParameters: ParserParameters[P]
  ): Either[String, List[Module]] =
    safeParse(Parsers.rep(new ModParser[P](parserParameters).mod), s)
      .map(_.map(FixNat.processModule))

}
