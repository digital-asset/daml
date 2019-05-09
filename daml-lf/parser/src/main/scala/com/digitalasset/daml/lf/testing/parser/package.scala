// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing

import com.digitalasset.daml.lf.archive.LanguageVersion
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.lfpackage.Ast.{Expr, Kind, Module, Type}

package object parser {

  val defaultLanguageVersion: LanguageVersion = LanguageVersion.default
  val defaultPkgId: PackageId = PackageId.assertFromString("-pkgId-")
  val defaultModName: ModuleName = DottedName.assertFromString("Mod")
  val defaultTemplName: TypeConName =
    Identifier(defaultPkgId, QualifiedName(defaultModName, DottedName.assertFromString("T")))

  private def safeParse[T](p: Parsers.Parser[T], s: String): Either[String, T] =
    try {
      Right(Parsers.parseAll(p, s))
    } catch {
      case e: ParserError =>
        Left(e.description)
    }

  def parseKind(s: String): Either[String, Kind] =
    safeParse(KindParser.kind, s)
  def parseType(s: String): Either[String, Type] =
    safeParse(TypeParser.typ, s)
  def parseExpr(s: String): Either[String, Expr] =
    safeParse(ExprParser.expr, s)
  def parseModules(s: String): Either[String, List[Module]] =
    safeParse(Parsers.rep(ModParser.mod), s)

}
