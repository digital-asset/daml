// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.speedy.Compiler
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConcurrentCompiledPackagesTestV2
    extends ConcurrentCompiledPackagesTest(
      LanguageVersion.defaultOrLatestStable(LanguageMajorVersion.V2)
    )

abstract class ConcurrentCompiledPackagesTest(languageVersion: LanguageVersion)
    extends AnyWordSpec
    with Matchers
    with Inside {

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultForMinor[this.type](languageVersion)

  s"ConcurrentCompiledPackages with ${languageVersion}" should {

    val pkg =
      p"""
        metadata ( 'pkg' : '1.0.0' )
        module Mod {
          val string: Text = "t";
        }
      """

    "load valid package" in {

      new ConcurrentCompiledPackages(Compiler.Config.Dev(languageVersion.major))
        .addPackage(parserParameters.defaultPackageId, pkg) shouldBe ResultDone(())

    }

    "not load of an invalid package" in {

      val packages = new ConcurrentCompiledPackages(Compiler.Config.Dev(languageVersion.major))

      val illFormedPackage =
        p"""
        metadata ( 'pkg' : '1.0.0' )
        module Mod {
          val string: Text = 1;
        }
      """;

      inside(packages.addPackage(parserParameters.defaultPackageId, illFormedPackage)) {
        case ResultError(Error.Package(_: Error.Package.Validation)) =>
      }
    }
  }
}
