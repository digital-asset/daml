// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.language.{LanguageMajorVersion, StablePackages}
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

class StablePackageTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with BazelRunfiles
    with Inspectors
    with TryValues {

  private def resource(path: String): File = {
    val f = new File(path).getAbsoluteFile
    require(f.exists, s"File does not exist: $f")
    f
  }

  for (majorVersion <- LanguageMajorVersion.All) {
    val stablePackages = StablePackages(majorVersion)

    s"LF $majorVersion" should {
      "DA.StablePackages" should {

        // We rely on the fact a dar generated with target x.dev contains all the stable packages
        lazy val darFile =
          resource(rlocation(s"daml-lf/archive/DarReaderTest-v${majorVersion.pretty}dev.dar"))
        lazy val depPkgs = UniversalArchiveDecoder.assertReadFile(darFile).dependencies.toMap

        // This should be all stable packages + `daml-prim` + `daml-stdlib`
        assert(depPkgs.size == stablePackages.allPackages.size + 2)

        "contains all the stable packages generated by the compiler" in {
          val pkgIdsInDar = depPkgs.keySet
          stablePackages.allPackages.foreach(pkg =>
            withClue(s"in ${pkg.name} ${pkg.moduleName}") {
              pkgIdsInDar should contain(pkg.packageId)
            }
          )
        }

        "lists the stable packages with their proper version" in {
          stablePackages.allPackages.foreach(pkg =>
            withClue(s"in ${pkg.name} ${pkg.moduleName} ${depPkgs(pkg.packageId).modules}") {
              pkg.languageVersion shouldBe depPkgs(pkg.packageId).languageVersion
            }
          )
        }

      }
    }
  }
}
