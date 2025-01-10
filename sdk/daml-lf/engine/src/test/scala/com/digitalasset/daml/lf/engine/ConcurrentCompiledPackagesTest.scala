// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.speedy.Compiler
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConcurrentCompiledPackagesTestV2
    extends ConcurrentCompiledPackagesTest(LanguageMajorVersion.V2)

class ConcurrentCompiledPackagesTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with Inside {

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)

  "ConcurrentCompiledPackages" should {

    val pkg =
      p"""
        metadata ( 'pkg' : '1.0.0' )
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
