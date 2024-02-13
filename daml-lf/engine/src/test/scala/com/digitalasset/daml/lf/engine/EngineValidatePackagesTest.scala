// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.daml.lf.testing.parser
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EngineValidatePackagesTestV2 extends EngineValidatePackagesTest(LanguageMajorVersion.V2)

class EngineValidatePackagesTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with Inside {

  val pkgId = Ref.PackageId.assertFromString("-pkg-")

  val langVersion = LanguageVersion.defaultOrLatestStable(majorLanguageVersion)

  implicit val parserParameters: parser.ParserParameters[this.type] =
    parser.ParserParameters(pkgId, langVersion)

  private def newEngine = new Engine(
    EngineConfig(LanguageVersion.AllVersions(majorLanguageVersion))
  )

  "Engine.validatePackages" should {

    val pkg =
      p"""
        metadata ( 'pkg' : '1.0.0' )
        module Mod {
          val string: Text = "t";
        }
      """

    "accept valid package" in {

      newEngine.validatePackages(Map(pkgId -> pkg)) shouldBe Right(())

    }

    "reject ill-typed packages" in {

      val illTypedPackage =
        p"""
        metadata ( 'pkg' : '1.0.0' )
        module Mod {
          val string: Text = 1;
        }
      """

      inside(
        newEngine.validatePackages(Map(pkgId -> illTypedPackage))
      ) { case Left(_: Error.Package.Validation) =>
      }

    }

    "reject non self-consistent sets of packages" in {

      val libraryId = Ref.PackageId.assertFromString("-library-")

      val dependentPackage =
        p"""
        metadata ( 'pkg' : '1.0.0' )
        module Mod {
          val string: Text = '-library-':Mod:Text;
        }
      """
          // TODO: parser should set dependencies properly
          .copy(directDeps = Set(libraryId))

      inside(newEngine.validatePackages(Map(pkgId -> dependentPackage))) {
        case Left(Error.Package.SelfConsistency(pkgIds, deps)) =>
          pkgIds shouldBe Set(pkgId)
          deps shouldBe Set(libraryId)
      }

    }

  }

}
