// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.config

import org.scalacheck.{Gen, Shrink}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import scopt.Read

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CustomScoptReadersSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {
  behavior of "CustomScoptReaders"

  implicit def noShrink[A]: Shrink[A] = Shrink.shrinkAny

  it should "parse a TemplateConfig" in forAll(genTemplateConfig) {
    templateConfig: TemplateConfig =>
      val sut = CustomScoptReaders.templateConfigRead
      val input: String = inputString(templateConfig)
      val actual: TemplateConfig = sut.reads(input)
      actual should ===(templateConfig)
  }

  it should "parse a list of TemplateConfigs" in forAll(Gen.nonEmptyListOf(genTemplateConfig)) {
    templateConfigs: List[TemplateConfig] =>
      import CustomScoptReaders.templateConfigRead
      val sut: Read[Seq[TemplateConfig]] = implicitly
      val input: String = inputString(templateConfigs)
      val actual: Seq[TemplateConfig] = sut.reads(input)
      actual.toList should ===(templateConfigs)
  }

  private def inputString(templateConfig: TemplateConfig): String =
    templateConfig.moduleName + ':'.toString + templateConfig.entityName

  private def inputString(templateConfigs: List[TemplateConfig]): String =
    templateConfigs.map(inputString).mkString(",")

  private def genTemplateConfig: Gen[TemplateConfig] =
    for {
      moduleName <- Gen.identifier
      entityName <- Gen.identifier
    } yield TemplateConfig(moduleName, entityName)
}
