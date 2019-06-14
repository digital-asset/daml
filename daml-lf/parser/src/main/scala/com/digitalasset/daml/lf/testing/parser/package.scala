// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast.{Expr, Kind, Module, Type}
import com.digitalasset.daml.lf.language.LanguageVersion

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
  def parseType(s: String)(implicit defaultPkgId: PackageId): Either[String, Type] =
    safeParse(new TypeParser().typ, s)
  def parseExpr(s: String)(implicit defaultPkgId: PackageId): Either[String, Expr] =
    safeParse(new ExprParser().expr, s)
  def parseModules(s: String)(
      implicit defaultPkgId: PackageId,
      languageVersion: LanguageVersion
  ): Either[String, List[Module]] =
    safeParse(Parsers.rep(new ModParser().mod), s)

}
