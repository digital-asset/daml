// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.config

import com.daml.extractor.config.Generators._
import org.scalacheck.{Gen, Shrink}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scopt.Read

class CustomScoptReadersSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  behavior of "CustomScoptReaders"

  implicit def noShrink[A]: Shrink[A] = Shrink.shrinkAny

  it should "parse a TemplateConfig" in forAll(genTemplateConfig) {
    templateConfig: TemplateConfig =>
      val sut = CustomScoptReaders.templateConfigRead
      val input: String = templateConfigUserInput(templateConfig)
      val actual: TemplateConfig = sut.reads(input)
      actual should ===(templateConfig)
  }

  it should "parse a list of TemplateConfigs" in forAll(Gen.nonEmptyListOf(genTemplateConfig)) {
    templateConfigs: List[TemplateConfig] =>
      import CustomScoptReaders.templateConfigRead
      val sut: Read[Seq[TemplateConfig]] = implicitly
      val input: String = templateConfigUserInput(templateConfigs)
      val actual: Seq[TemplateConfig] = sut.reads(input)
      actual.toList should ===(templateConfigs)
  }
}
