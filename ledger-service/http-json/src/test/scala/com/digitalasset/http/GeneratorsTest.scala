// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.http.Generators.genDuplicateApiIdentifiers
import org.scalacheck.Shrink
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class GeneratorsTest extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {
  import Shrink.shrinkAny

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 10000)

  "Generators.genDuplicateApiIdentifiers" should "generate only duplicate API Identifiers" in
    forAll(genDuplicateApiIdentifiers) { ids =>
      ids.size should be >= 2
      ids.sliding(2).foreach {
        case List(a1, a2) =>
          a1.packageId should not be a2.packageId
          (a1.moduleName, a1.entityName) should be(a2.moduleName -> a2.entityName)
        case x @ _ => fail(s"Should never happen when sliding(2): $x")
      }
    }
}
