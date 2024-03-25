// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class DamlPackageLoaderTest extends AnyWordSpec with BaseTest {

  "DamlPackageLoader" should {
    "find daml package" in {
      for {
        packages <- DamlPackageLoader.getPackagesFromDarFile(CantonExamplesPath)
      } yield packages.values.flatMap(_.modules.keys.map(_.toString)) should contain.allOf(
        "CantonExamples",
        "Divulgence",
        "Paint",
      )
    }
  }
}
