// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{DottedName, Identifier, PackageId, QualifiedName}
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.language.{Ast, LanguageVersion => LV}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.daml.lf.language

class DependencyVersionSpec extends AnyWordSpec with TableDrivenPropertyChecks with Matchers {

  private[this] val v2_1 = LV(LV.Major.V2, LV.Minor("1"))
  private[this] val v2_dev = LV(LV.Major.V2, LV.Minor("dev"))
  private[this] val A = (PackageId.assertFromString("-pkg1-"), DottedName.assertFromString("A"))
  private[this] val B = (PackageId.assertFromString("-pkg2-"), DottedName.assertFromString("B"))
  private[this] val E = (PackageId.assertFromString("-pkg3-"), DottedName.assertFromString("E"))
  private[this] val u = DottedName.assertFromString("u")

  "Dependency validation should detect cycles between modules" in {

    def pkg(
        ref: (PackageId, DottedName),
        langVersion: LV,
        depRefs: (PackageId, DottedName)*
    ) = {
      val (pkgId, modName) = ref

      val mod = Module.build(
        name = modName,
        definitions = (u -> DValue(TUnit, EUnit, false)) +:
          depRefs.map { case (depPkgId, depModName) =>
            depModName -> DValue(
              TUnit,
              EVal(Identifier(depPkgId, QualifiedName(depModName, u))),
              false,
            )
          },
        templates = List.empty,
        exceptions = List.empty,
        interfaces = List.empty,
        featureFlags = FeatureFlags.default,
      )

      pkgId -> Package(
        Map(modName -> mod),
        depRefs.iterator.map(_._1).toSet - pkgId,
        langVersion,
        Ast.PackageMetadata(
          Ref.PackageName.assertFromString("foo"),
          Ref.PackageVersion.assertFromString("0.0.0"),
          None,
        ),
      )
    }

    val negativeTestCases = Table(
      "valid packages",
      Map(pkg(A, v2_dev, A, B, E), pkg(B, v2_1, B, E), pkg(E, v2_1, E)),
      Map(pkg(A, v2_dev, A, B, E), pkg(B, v2_dev, B, E), pkg(E, v2_1, E)),
    )

    val postiveTestCase = Table(
      ("invalid module", "packages"),
      A -> Map(pkg(A, v2_1, A, B, E), pkg(B, v2_dev, B, E), pkg(E, v2_1, E)),
      A -> Map(pkg(A, v2_1, A, B, E), pkg(B, v2_1, B, E), pkg(E, v2_dev, E)),
      B -> Map(pkg(A, v2_dev, A, B, E), pkg(B, v2_1, B, E), pkg(E, v2_dev, E)),
    )

    forEvery(negativeTestCases) { pkgs =>
      pkgs.foreach { case (pkgId, pkg) =>
        DependencyVersion.checkPackage(language.PackageInterface(pkgs), pkgId, pkg)
      }
    }

    forEvery(postiveTestCase) { case ((pkgdId, _), pkgs) =>
      an[EModuleVersionDependencies] should be thrownBy
        DependencyVersion.checkPackage(language.PackageInterface(pkgs), pkgdId, pkgs(pkgdId))
    }

  }

}
