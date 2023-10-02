// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.daml.lf.speedy.Compiler
import com.daml.lf.testing.parser
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConcurrentCompiledPackagesTest extends AnyWordSpec with Matchers with Inside {

  for (majorLanguageVersion <- LanguageMajorVersion.All) {

    implicit val parserParameters: parser.ParserParameters[this.type] =
      parser.ParserParameters(
        parser.defaultPackageId,
        // TODO(#17366): use something like LanguageVersion.default(major) after the refactoring of
        //  LanguageVersion
        majorLanguageVersion match {
          case LanguageMajorVersion.V1 => LanguageVersion.default
          case LanguageMajorVersion.V2 => LanguageVersion.v2_dev
        },
      )

    s"LF $majorLanguageVersion" should {

      "ConcurrentCompiledPackages" should {

        val pkg =
          p"""
        module Mod {
          val string: Text = "t";
        }
      """

        "load valid package" in {

          new ConcurrentCompiledPackages(Compiler.Config.Dev(majorLanguageVersion))
            .addPackage(parserParameters.defaultPackageId, pkg) shouldBe ResultDone(())

        }

        "not load of an invalid package" in {

          val packages = new ConcurrentCompiledPackages(Compiler.Config.Dev(majorLanguageVersion))

          val illFormedPackage =
            p"""
        module Mod {
          val string: Text = 1;
        }
      """;

          inside(packages.addPackage(parserParameters.defaultPackageId, illFormedPackage)) {
            case ResultError(Error.Package(_: Error.Package.Validation)) =>
          }
        }

        "not load of a package with disallowed language version" in {

          val packages = new ConcurrentCompiledPackages(
            Compiler.Config.Default.copy(allowedLanguageVersions = LanguageVersion.LegacyVersions)
          )

          assert(!LanguageVersion.LegacyVersions.contains(parserParameters.languageVersion))

          inside(packages.addPackage(parserParameters.defaultPackageId, pkg)) {
            case ResultError(Error.Package(err: Error.Package.AllowedLanguageVersion)) =>
              err.packageId shouldBe parserParameters.defaultPackageId
              err.languageVersion shouldBe parserParameters.languageVersion
              err.allowedLanguageVersions shouldBe LanguageVersion.LegacyVersions
          }
        }
      }
    }
  }
}
