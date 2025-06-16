// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.testing.parser
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EngineValidatePackagesTestV2 extends EngineValidatePackagesTest(LanguageMajorVersion.V2)

class EngineValidatePackagesTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with Inside {

  val pkgId = Ref.PackageId.assertFromString("-pkg-")
  val extraPkgId = Ref.PackageId.assertFromString("-extra-")
  val missingPkgId = Ref.PackageId.assertFromString("-missing-")

  val langVersion = LanguageVersion.defaultOrLatestStable(majorLanguageVersion)

  implicit val parserParameters: parser.ParserParameters[this.type] =
    parser.ParserParameters(pkgId, langVersion)

  private def newEngine = new Engine(
    EngineConfig(LanguageVersion.AllVersions(majorLanguageVersion))
  )

  private def darFromPackageMap(
      mainPkg: (Ref.PackageId, Package),
      dependentPkgs: (Ref.PackageId, Package)*
  ): Dar[(Ref.PackageId, Package)] =
    Dar(mainPkg, dependentPkgs.toList)

  "Engine.validatePackages" should {
    val pkg =
      p"""
        metadata ( 'pkg' : '1.0.0' )
        module Mod {
          val string: Text = "t";
        }
      """

    "accept valid package" in {
      newEngine.validateDar(darFromPackageMap(pkgId -> pkg)) shouldBe Right(())
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
        newEngine.validateDar(darFromPackageMap(pkgId -> illTypedPackage))
      ) { case Left(_: Error.Package.Validation) =>
      }
    }

    "reject non self-consistent sets of packages" should {
      val extraPkg =
        p"""
           metadata ( 'extra' : '1.0.0' )
           module Mod {
             val string: Text = "e";
           }
         """

      "with missing dependencies only" in {
        val dependentPackage =
          p"""
              metadata ( 'pkg' : '1.0.0' )
              module Mod {
                val string: Text = '-missing-':Mod:Text;
              }
            """
            // TODO: parser should set dependencies properly
            .copy(directDeps = Set(missingPkgId))

        inside(newEngine.validateDar(darFromPackageMap(pkgId -> dependentPackage))) {
          case Left(Error.Package.SelfConsistency(pkgIds, missingDeps, extraDeps)) =>
            pkgIds shouldBe Set(pkgId)
            missingDeps shouldBe Set(missingPkgId)
            extraDeps shouldBe Set.empty
        }
      }

      "with extra dependencies only" in {
        val dependentPackage =
          p"""
              metadata ( 'pkg' : '1.0.0' )
              module Mod {
                val string: Text = "t";
              }
            """

        inside(
          newEngine.validateDar(
            darFromPackageMap(pkgId -> dependentPackage, extraPkgId -> extraPkg)
          )
        ) { case Left(Error.Package.SelfConsistency(pkgIds, missingDeps, extraDeps)) =>
          pkgIds shouldBe Set(pkgId, extraPkgId)
          missingDeps shouldBe Set.empty
          extraDeps shouldBe Set(extraPkgId)
        }
      }

      "with both missing dependencies and extra dependencies" in {
        val dependentPackage =
          p"""
              metadata ( 'pkg' : '1.0.0' )
              module Mod {
                val string: Text = '-missing-':Mod:Text;
              }
            """
            // TODO: parser should set dependencies properly
            .copy(directDeps = Set(missingPkgId))

        inside(
          newEngine.validateDar(
            darFromPackageMap(pkgId -> dependentPackage, extraPkgId -> extraPkg)
          )
        ) { case Left(Error.Package.SelfConsistency(pkgIds, missingDeps, extraDeps)) =>
          pkgIds shouldBe Set(pkgId, extraPkgId)
          missingDeps shouldBe Set(missingPkgId)
          extraDeps shouldBe Set(extraPkgId)
        }
      }
    }
  }
}
