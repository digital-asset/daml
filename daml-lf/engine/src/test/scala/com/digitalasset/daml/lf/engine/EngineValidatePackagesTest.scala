// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref
import com.daml.lf.language.LanguageVersion
import com.daml.lf.testing.parser.Implicits._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EngineValidatePackagesTest extends AnyWordSpec with Matchers with Inside {

  import defaultParserParameters.{defaultPackageId => pkgId, languageVersion => langVersion}

  private def newEngine = new Engine(EngineConfig(LanguageVersion.DevVersions))

  "Engine.validatePackages" should {

    val pkg =
      p"""
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
        module Mod {
          val string: Text = 1;
        }
      """

      inside(
        newEngine.validatePackages(Map(pkgId -> illTypedPackage))
      ) { case Left(_: Error.Package.Validation) =>
      }

    }

    "reject packages with disallowed language version" in {

      val engine = new Engine(EngineConfig(LanguageVersion.LegacyVersions))

      assert(!LanguageVersion.LegacyVersions.contains(langVersion))

      inside(engine.validatePackages(Map(pkgId -> pkg))) {
        case Left(err: Error.Package.AllowedLanguageVersion) =>
          err.packageId shouldBe pkgId
          err.languageVersion shouldBe langVersion
          err.allowedLanguageVersions shouldBe LanguageVersion.LegacyVersions
      }

    }

    "reject non self-consistent sets of packages" in {

      val libraryId = Ref.PackageId.assertFromString("-library-")

      val dependentPackage =
        p"""
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
