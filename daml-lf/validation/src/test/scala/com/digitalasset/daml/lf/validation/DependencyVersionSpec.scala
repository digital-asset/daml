// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package validation

import com.daml.lf.data.Ref.{DottedName, Identifier, PackageId, QualifiedName}
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.language.{LanguageVersion => LV}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DependencyVersionSpec extends AnyWordSpec with TableDrivenPropertyChecks with Matchers {

  private[this] val v1_6 = LV(LV.Major.V1, LV.Minor("6"))
  private[this] val v1_7 = LV(LV.Major.V1, LV.Minor("7"))
  private[this] val v1_8 = LV(LV.Major.V1, LV.Minor("8"))
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
        None,
      )
    }

    val negativeTestCases = Table(
      "valid packages",
      Map(pkg(A, v1_8, A, B, E), pkg(B, v1_7, B, E), pkg(E, v1_6, E)),
      Map(pkg(A, v1_8, A, B, E), pkg(B, v1_8, B, E), pkg(E, v1_6, E)),
    )

    val postiveTestCase = Table(
      ("invalid module", "packages"),
      A -> Map(pkg(A, v1_6, A, B, E), pkg(B, v1_7, B, E), pkg(E, v1_6, E)),
      A -> Map(pkg(A, v1_7, A, B, E), pkg(B, v1_7, B, E), pkg(E, v1_8, E)),
      B -> Map(pkg(A, v1_8, A, B, E), pkg(B, v1_6, B, E), pkg(E, v1_7, E)),
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
