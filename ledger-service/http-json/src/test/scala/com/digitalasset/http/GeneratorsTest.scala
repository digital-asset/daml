// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.Generators.genDuplicateApiIdentifiers
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class GeneratorsTest extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 10000)

  import org.scalacheck.Shrink.shrinkAny

  "Generators.genDuplicateApiIdentifiers" should "generate API Identifiers with the same moduleName and entityName" in
    forAll(genDuplicateApiIdentifiers) { ids =>
      ids.size should be >= 2
      val (packageIds, moduleNames, entityNames) =
        ids.foldLeft((Set.empty[String], Set.empty[String], Set.empty[String])) { (b, a) =>
          (b._1 + a.packageId, b._2 + a.moduleName, b._3 + a.entityName)
        }

      packageIds.size shouldBe ids.size
      moduleNames.size shouldBe 1
      entityNames.size shouldBe 1
    }
}
