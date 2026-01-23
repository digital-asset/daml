// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

import zio.internal.stacktracer.SourceLocation
import zio.test
import zio.test.*

import scala.language.implicitConversions

object IdentifierFilterSpec extends ZIOSpecDefault:
  def spec = suite("IdentifierFilter")(
    test("toString")(
      assertTrue(
        IdentifierFilter.AcceptAll.toString == "*",
        IdentifierFilter.RejectAll.toString == "!*",
        toStringRoundtrip("*"),
        toStringRoundtrip("!pkgName"),
        toStringRoundtrip("!pkgName:moduleName"),
        toStringRoundtrip("!pkgName:*"),
        toStringRoundtrip("!pkgName:moduleName:entityName"),
        toStringRoundtrip("pkgName:moduleName:entityName"),
        toStringRoundtrip("pkgName1|pkgName2@>=1.0.0", "pkgName1 | pkgName2@>=1.0.0"),
        toStringRoundtrip(
          "(pkgName1:mod1|pkgName2:mod2)&pkgName3",
          "(pkgName1:mod1 | pkgName2:mod2) & pkgName3",
        ),
        toStringRoundtrip(
          "pkgName1:mod1|pkgName2:mod2&pkgName3",
          "pkgName1:mod1 | pkgName2:mod2 & pkgName3",
        ),
        toStringRoundtrip("* & !(Interfaces.W | Templates.P)"),
      )
    ),
    test("Failures")(
      TestResult.allSuccesses(
        "pkgName@1-b" givesError "Unexpected [-b] at position 9, expected one of [<END>,|,&,:]",
        "pkgName@" givesError "Unexpected [<END>] at position 8, expected one of [<package version>,<package id>]",
        "pkgName@deadbeef" givesError "Unexpected [deadbeef] at position 8, expected one of [<package version>,<package id>]",
        "pkg|" givesError "Unexpected [<END>] at position 4, expected one of [*,<name>,<package name>,(,!]",
        "pkg*" givesError "Unexpected [*] at position 3, expected one of [<END>,|,&,:,@]",
        "pkg:Module*" givesError "Unexpected [*] at position 10, expected one of [<END>,|,&,:]",
        "pkg:Module:Entity*" givesError "Unexpected [*] at position 17, expected one of [<END>,|,&]",
        "pkg:A. B:Entity" givesError "Unexpected [. B:Entity] at position 5, expected one of [<END>,|,&,:]",
        "pkg:A.B:Entity. Sub" givesError "Unexpected [. Sub] at position 14, expected one of [<END>,|,&]",
        "pkg:Module:Entity. " givesError "Unexpected [. ] at position 17, expected one of [<END>,|,&]",
      )
    ),
    filter("*")(accepts("pkg:Module:Entity")),
    filter("!*")(rejects("pkg:Module:Entity")),
    filter("pkg")(accepts("pkg:Module:Entity@2.0.0")),
    filter("pkg@>=1.0.1")(
      accepts("pkg:Module:Entity@2.0.0", "pkg:Module:Entity@3.0.0"),
      rejects("pkg:Module:Entity@1.0.0"),
    ),
    filter("pkg@~2")(
      accepts("pkg:Module:Entity@2.0.0"),
      rejects("pkg:Module:Entity@1.0.0", "pkg:Module:Entity@3.0.0"),
    ),
    filter("!pkg@<1.0.1")(
      accepts("pkg:Module:Entity@2.0.0", "pkg:Module:Entity@3.0.0", "other:Module:Entity@0.0.1"),
      rejects("pkg:Module:Entity@1.0.0"),
    ),
    filter("pkg@abcd123456789012345678901234567890123456789012345678901234567890")(
      accepts("pkg:Module:Entity@abcd123456789012345678901234567890123456789012345678901234567890"),
      rejects(
        "pkg:Module:Entity@fedc123456789012345678901234567890123456789012345678901234567890",
        "other:Module:Entity",
        "pkg:Module:Entity",
      ),
    ),
    // older ledgers with package ids as package names
    filter("abcd123456789012345678901234567890123456789012345678901234567890")(
      accepts(
        Identifier(
          PackageId("abcd123456789012345678901234567890123456789012345678901234567890"),
          PackageName("abcd123456789012345678901234567890123456789012345678901234567890"),
          PackageVersion.Unknown,
          ModuleName("Module"),
          EntityName("Entity"),
        )
      ),
      rejects(
        Identifier(
          PackageId("fdec123456789012345678901234567890123456789012345678901234567890"),
          PackageName("fdec123456789012345678901234567890123456789012345678901234567890"),
          PackageVersion.Unknown,
          ModuleName("Module"),
          EntityName("Entity"),
        )
      ),
    ),
    filter("pkg:Module:Entity")(
      accepts("pkg:Module:Entity"),
      rejects("pkg:Module:FooBar"),
    ),
    filter("pkg:*:Entity")(
      accepts("pkg:Foo:Entity", "pkg:Bar:Entity"),
      rejects("pkg:Foo:Exclude"),
    ),
    filter("pkg & !pkg:Banned")(
      accepts("pkg:Foo:Bar"),
      rejects("pkg:Banned:Bar", "other:Irrelevant:Thing"),
    ),
    filter("*:This | *:That")(
      accepts("pkg:This:Foo", "pkg:That:Bar"),
      rejects("pkg:Banned:FooBar"),
    ),
    filter("*:Foo.Bar:* & *:Foo.Bar:OtherThing")(
      accepts("pkg:Foo.Bar:OtherThing"),
      rejects("pkg:Foo.Bar:Thing"),
    ),
    filter("*:A.B:Foo & !*:A.B:Bar | *:A.B:Baz")(
      accepts("pkg:A.B:Foo", "pkg:A.B:Baz"),
      rejects("pkg:A.B:Bar"),
    ),
    filter("*:A.B:Foo & *:A.B:Bar | *:A.B:Baz")(
      accepts("pkg:A.B:Baz"),
      rejects("pkg:A.B:Foo", "pkg:A.B:Bar"),
    ),
    filter("*:A.B:Foo & (*:A.B:Bar | *:A.B:Baz)")(
      rejects("pkg:A.B:Foo", "pkg:A.B:Bar", "pkg:A.B:Baz")
    ),
    filter("* & !(Interfaces.W | Templates.P)")(
      accepts("pkg:Interfaces:A"),
      rejects("pkg:Interfaces:W", "pkg:Templates:P"),
    ),
    filter("A.*")(
      accepts("pkg:A:Foo", "pkg:A.A:Foo", "pkg:A:Bar"),
      rejects("pkg:B:Foo", "pkg:AA:Bar"),
    ),
  )

  given Conversion[Identifier, Identifier] = identity
  given Conversion[String, Identifier] =
    val p = "([^@:]+):([^@:]+):([^@:]+)(?:@(.+))?".r
    {
      case p(pkgName, moduleName, entityName, pkgId) if Option(pkgId).exists(_.sizeIs == 64) =>
        Identifier(
          PackageId(pkgId),
          PackageName(pkgName),
          PackageVersion.Unknown,
          ModuleName(moduleName),
          EntityName(entityName),
        )
      case p(pkgName, moduleName, entityName, pkgVersion) =>
        Identifier(
          PackageId("deadbeef"),
          PackageName(pkgName),
          Option(pkgVersion).map(PackageVersion).getOrElse(PackageVersion.Unknown),
          ModuleName(moduleName),
          EntityName(entityName),
        )
    }

  inline private def toStringRoundtrip(str: String, expected: String = ""): Boolean =
    if expected.isEmpty
    then IdentifierFilter.assertFromString(str).toString == str
    else IdentifierFilter.assertFromString(str).toString == expected

  extension (str: String)
    inline infix private def givesError(inline error: String): TestResult =
      assert(IdentifierFilter.fromString(str))(Assertion.isLeft(Assertion.equalTo(error)))

  extension (id: Identifier)
    private def pretty =
      if id.packageVersion == PackageVersion.Unknown
      then s"${id.packageName}:${id.moduleName}:${id.entityName}"
      else s"${id.packageName}@${id.packageVersion}:${id.moduleName}:${id.entityName}"

  private def filter(str: String)(tests: Assertion[IdentifierFilter]*)(implicit
      sourceLocation: SourceLocation
  ) =
    test(s"Filter $str")(assert(IdentifierFilter.assertFromString(str))(tests.reduce(_ && _)))
  private def accepts[A](ids: A*)(using Conversion[A, Identifier]) =
    ids
      .map(id =>
        Assertion.assertion[IdentifierFilter](s"${id.convert.pretty} accepted")(f => f(id))
      )
      .reduce(_ && _)
  private def rejects[A](ids: A*)(using Conversion[A, Identifier]) =
    ids
      .map(id =>
        Assertion.assertion[IdentifierFilter](s"${id.convert.pretty} rejected")(f => !f(id))
      )
      .reduce(_ && _)
