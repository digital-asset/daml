// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.config

import com.digitalasset.extractor.config.Generators.genTemplateConfig
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class TemplateConfigSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  behavior of TemplateConfig.getClass.getSimpleName

  it should "be sortable" in forAll(Gen.nonEmptyListOf(genTemplateConfig)) { templateConfigs =>
    whenever(templateConfigs.size > 1) {
      val sorted = templateConfigs.sorted
      sorted.sliding(2).forall {
        case List(x, y) => x.compareTo(y) <= 0
      } should ===(true)
    }
  }

  it should "have deterministic sort" in forAll(Gen.nonEmptyListOf(genTemplateConfig)) { as =>
    as.reverse.sorted should ===(as.sorted)
    as.sorted.sorted should ===(as.sorted)
  }
}
