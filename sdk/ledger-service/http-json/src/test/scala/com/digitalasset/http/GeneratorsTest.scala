// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.Generators.genDuplicateModuleEntityApiIdentifiers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class GeneratorsTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 10000)

  import org.scalacheck.Shrink.shrinkAny

  "Generators.genDuplicateApiIdentifiers" should "generate API Identifiers with the same moduleName and entityName" in
    forAll(genDuplicateModuleEntityApiIdentifiers) { ids =>
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
