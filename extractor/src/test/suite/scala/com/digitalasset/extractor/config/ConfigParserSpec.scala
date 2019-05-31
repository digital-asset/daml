// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.config

import com.digitalasset.extractor.config.Generators._
import com.digitalasset.extractor.targets.Target
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Inside, Matchers}

class ConfigParserSpec
    extends FlatSpec
    with Matchers
    with Inside
    with GeneratorDrivenPropertyChecks {
  behavior of ConfigParser.getClass.getSimpleName

  val requiredArgs = Vector("--party", "Bob")

  it should "parse template configuration" in forAll(Gen.nonEmptyListOf(genTemplateConfig)) {
    templateConfigs: List[TemplateConfig] =>
      val args = requiredArgs ++ Vector("--templates", templateConfigUserInput(templateConfigs))
      inside(ConfigParser.parse(args)) {
        case Some((config, _)) =>
          config.templateConfigs should ===(templateConfigs.toSet)
      }
  }

  it should "fail parsing when duplicate template configurations" in forAll(
    Gen.nonEmptyListOf(genTemplateConfig)) { templateConfigs: List[TemplateConfig] =>
    val duplicate = templateConfigs.headOption.getOrElse(
      fail("expected non empty list of TemplateConfig objects"))

    val args = requiredArgs ++ Vector(
      "--templates",
      templateConfigUserInput(duplicate :: templateConfigs))

    // scopt prints errors into STD Error stream
    val capturedStdErr = new java.io.ByteArrayOutputStream()
    val result: Option[(ExtractorConfig, Target)] = Console.withErr(capturedStdErr) {
      ConfigParser.parse(args)
    }
    capturedStdErr.flush()
    capturedStdErr.close()
    result should ===(None)
    val firstLine = capturedStdErr.toString.split('\n')(0)
    firstLine should ===("Error: The list of templates must contain unique elements")
  }
}
