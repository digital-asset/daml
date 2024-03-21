// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.language.LanguageVersion
import com.daml.lf.speedy.Compiler
import com.daml.lf.testing.parser.Implicits._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConcurrentCompiledPackagesTest extends AnyWordSpec with Matchers with Inside {

  "ConcurrentCompiledPackages" should {

    val pkg =
      p"""
        module Mod { 
          val string: Text = "t";
        }
      """

    "load valid package" in {

      new ConcurrentCompiledPackages(Compiler.Config.Dev)
        .addPackage(defaultParserParameters.defaultPackageId, pkg) shouldBe ResultDone(())

    }

    "not load of an invalid package" in {

      val packages = new ConcurrentCompiledPackages(Compiler.Config.Dev)

      val illFormedPackage =
        p"""
        module Mod { 
          val string: Text = 1;
        }
      """;

      inside(packages.addPackage(defaultParserParameters.defaultPackageId, illFormedPackage)) {
        case ResultError(Error.Package(_: Error.Package.Validation)) =>
      }
    }

    "not load of a package with disallowed language version" in {

      val packages = new ConcurrentCompiledPackages(
        Compiler.Config.Default.copy(allowedLanguageVersions = LanguageVersion.LegacyVersions)
      )

      assert(!LanguageVersion.LegacyVersions.contains(defaultParserParameters.languageVersion))

      inside(packages.addPackage(defaultParserParameters.defaultPackageId, pkg)) {
        case ResultError(Error.Package(err: Error.Package.AllowedLanguageVersion)) =>
          err.packageId shouldBe defaultParserParameters.defaultPackageId
          err.languageVersion shouldBe defaultParserParameters.languageVersion
          err.allowedLanguageVersions shouldBe LanguageVersion.LegacyVersions
      }
    }

  }

}
