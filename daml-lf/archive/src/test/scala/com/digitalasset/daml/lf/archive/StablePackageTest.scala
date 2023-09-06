// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.language.StablePackage
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

  // TODO(#17366): For now both 1.dev and 2.dev contain the same stable packages. Once 2.dev
  //    diverges from 1.dev we will need to split this test.
  for (version <- Seq("v1dev", "v2dev")) {
    version should {
      "DA.StablePackages" should {

        // We rely on the fact a dar generated with target x.dev contains all the stable packages
        lazy val darFile = resource(rlocation(s"daml-lf/archive/DarReaderTest-${version}.dar"))
        lazy val depPkgs = UniversalArchiveDecoder.assertReadFile(darFile).dependencies.toMap

        // This should be all stable packages + `daml-prim` + `daml-stdlib`
        assert(depPkgs.size == StablePackage.values.size + 2)

        "contains all the stable packages generated by the compiler" in {
          val pkgIdsInDar = depPkgs.keySet
          StablePackage.values.foreach(pkg => pkgIdsInDar should contain(pkg.packageId))
        }

        "lists the stable packages with their proper version" in {
          StablePackage.values.foreach(pkg =>
            pkg.languageVersion shouldBe depPkgs(pkg.packageId).languageVersion
          )
        }

      }
    }
  }

}
