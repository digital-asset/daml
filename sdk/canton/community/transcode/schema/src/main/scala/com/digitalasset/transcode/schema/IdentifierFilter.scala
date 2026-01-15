// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

import org.semver4j.Semver

import scala.annotation.targetName
import scala.collection.mutable

trait IdentifierFilter extends (Identifier => Boolean) { self =>
  @targetName("or") private def `||`(that: IdentifierFilter): IdentifierFilter =
    new IdentifierFilter:
      def apply(id: Identifier): Boolean = self(id) || that(id)
      override def toString: String = s"${self.toString} | ${that.toString}"

  @targetName("and") private def `&&`(that: IdentifierFilter): IdentifierFilter =
    new IdentifierFilter:
      def apply(id: Identifier): Boolean = self(id) && that(id)
      override def toString: String = s"${self.toString} & ${that.toString}"

  @targetName("not") private def `unary_!` : IdentifierFilter = new IdentifierFilter:
    def apply(id: Identifier): Boolean = !self(id)
    override def toString: String = s"!${self.toString}"

  private def wrapInParenthesis: IdentifierFilter = new IdentifierFilter:
    def apply(id: Identifier): Boolean = self(id)
    override def toString: String = s"(${self.toString})"

  private def withName(name: String) = new IdentifierFilter:
    def apply(id: Identifier): Boolean = self(id)
    override def toString: String = name
}

object IdentifierFilter:
  private val star: IdentifierFilter = _ => true
  val AcceptAll: IdentifierFilter = star.withName("*")
  val RejectAll: IdentifierFilter = !AcceptAll

  def fromString(filter: String): Either[String, IdentifierFilter] =
    if filter == AcceptAll.toString then Right(AcceptAll)
    else if filter == RejectAll.toString then Right(RejectAll)
    else
      fastparse
        .parse(filter, ctx => parser.fullParser(using ctx), verboseFailures = true)
        .fold(
          onFailure = (_, _, extra) =>
            val traced = extra.trace()
            val unexpected =
              if traced.index >= traced.input.length
              then "<END>"
              else traced.input.slice(traced.index, traced.input.length)
            val expected = traced.terminals.value.map(_.force).distinct.mkString("[", ",", "]")
            Left(s"Unexpected [$unexpected] at position ${traced.index}, expected one of $expected")
          ,
          onSuccess = (v, _) => Right(v),
        )

  def assertFromString(filter: String): IdentifierFilter =
    fromString(filter).fold(ex => throw Exception(ex), identity)

  private object parser {
    import fastparse.SingleLineWhitespace.*
    import fastparse.{P, *}

    def fullParser[$: P]: P[IdentifierFilter] = Start ~ union ~ End.opaque("<END>")

    private def union[$: P]: P[IdentifierFilter] =
      intersection
        .rep(min = 1, sep = "|".literal./)
        .map(filters => filters.reduceOption(_ || _).getOrElse(IdentifierFilter.RejectAll))

    private def intersection[$: P]: P[IdentifierFilter] =
      (negation | value)
        .rep(min = 1, sep = "&".literal./)
        .map(filters => filters.reduceOption(_ && _).getOrElse(IdentifierFilter.AcceptAll))

    private def negation[$: P]: P[IdentifierFilter] =
      ("!".literal ~/ value).map(!_)

    private def value[$: P]: P[IdentifierFilter] =
      parens | fqnIdentifierFilter | simplifiedFqnFilter | acceptAllFilter

    private def parens[$: P]: P[IdentifierFilter] =
      ("(".literal ~/ union ~ ")".literal).map(f => f.wrapInParenthesis)

    private def acceptAllFilter[$: P]: P[IdentifierFilter] =
      "*".literal.map(_ => AcceptAll)

    private def fqnIdentifierFilter[$: P]: P[IdentifierFilter] =
      (packageIdentifier ~~ (":".literal ~~/ moduleIdentifier ~~ (":".literal ~~/ entityIdentifier).?).?).collect {
        case (pkg, Some((mod, Some(ent)))) => pkg && mod && ent
        case (pkg, Some((mod, None))) => pkg && mod
        case (pkg, None) => pkg
      }.captureFilterName

    private def packageIdentifier[$: P]: P[IdentifierFilter] =
      acceptAllFilter | (packageName ~~ ("@".literal ~~/ packageIdOrPackageVersion).?).map(
        (pkgName, pkgIdOrVer) => pkgName && pkgIdOrVer.getOrElse(AcceptAll)
      )
    private def moduleIdentifier[$: P]: P[IdentifierFilter] =
      acceptAllFilter | dottedName
        .opaque("<module name>")
        .map(moduleName => id => id.moduleName.moduleName == ModuleName(moduleName))
    private def entityIdentifier[$: P]: P[IdentifierFilter] =
      acceptAllFilter | dottedName
        .opaque("<entity name>")
        .map(entityName => id => id.entityName.entityName == EntityName(entityName))

    private def packageName[$: P]: P[IdentifierFilter] =
      (name.repX(min = 1, sep = "-").! ~~ !".")
        .opaque("<package name>")
        .map(pkgName => id => id.packageName == PackageName(pkgName))

    private def packageIdOrPackageVersion[$: P]: P[IdentifierFilter] =
      packageIdFilter.opaque("<package id>") | packageVersionFilter.opaque("<package version>")

    private def packageIdFilter[$: P]: P[IdentifierFilter] =
      CharIn("0-9a-f").repX(exactly = 64).!.map { packageId => id =>
        id.packageId == PackageId(packageId)
      }

    private def packageVersionFilter[$: P]: P[IdentifierFilter] =
      CharIn("=<>~*^[]()~.0-9,").repX(min = 1).!.map { versionReq =>
        val cache = mutable.Map.empty[PackageVersion, Boolean]
        id =>
          cache.get(id.packageVersion) match
            case Some(satisfies) => satisfies
            case None =>
              cache.synchronized {
                val satisfies =
                  Option(Semver.parse(id.packageVersion)).exists(_.satisfies(versionReq))
                cache(id.packageVersion) = satisfies
                satisfies
              }

      }

    private def simplifiedFqnFilter[$: P]: P[IdentifierFilter] =
      (dottedName ~~ ".*".literal.!.?)
        .map[IdentifierFilter]((prefix, star) =>
          id =>
            if star.nonEmpty
            then s"${id.moduleName}.${id.entityName}.".startsWith(s"$prefix.")
            else s"${id.moduleName}.${id.entityName}" == prefix
        )
        .captureFilterName

    private def dottedName[$: P]: P[String] = name.repX(min = 1, sep = ".".literal).!
    private def name[$: P]: P[String] = CharIn("a-zA-Z0-9_'").repX(min = 1).!.opaque("<name>")

    extension (parser0: ParsingRun[?] ?=> ParsingRun[IdentifierFilter])
      private def captureFilterName = (ctx: ParsingRun[?]) ?=>
        val start = ctx.index
        parser0.map(f => f.withName(ctx.input.slice(start, ctx.index)))
    extension (inline str: String) inline private def literal[$: P]: P[Unit] = str.opaque(str)
  }
