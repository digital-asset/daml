// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.config

import com.daml.extractor.config.Generators._
import com.daml.extractor.targets.Target
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.OneAnd
import scalaz.Scalaz._
import scalaz.scalacheck.ScalazArbitrary._

class ConfigParserSpec
    extends AnyFlatSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks {

  behavior of ConfigParser.getClass.getSimpleName

  val requiredArgs = Vector("--party", "Bob")

  it should "parse template configuration" in forAll {
    templateConfigs: OneAnd[List, TemplateConfig] =>
      val args = requiredArgs ++ Vector("--templates", templateConfigUserInput(templateConfigs))
      inside(ConfigParser.parse(args)) {
        case Some((config, _)) =>
          config.templateConfigs should ===(templateConfigs.toSet)
      }
  }

  it should "fail parsing when duplicate template configurations" in forAll {
    templateConfigs: OneAnd[List, TemplateConfig] =>
      val duplicate = templateConfigs.head

      val args = requiredArgs ++ Vector(
        "--templates",
        templateConfigUserInput(duplicate :: templateConfigs.toList))

      // scopt prints errors into STD Error stream
      val capturedStdErr = new java.io.ByteArrayOutputStream()
      val result: Option[(ExtractorConfig, Target)] = Console.withErr(capturedStdErr) {
        ConfigParser.parse(args)
      }
      capturedStdErr.flush()
      capturedStdErr.close()
      result should ===(None)
      val firstLine = capturedStdErr.toString.replaceAllLiterally("\r", "").split('\n')(0)
      firstLine should ===("Error: The list of templates must contain unique elements")
  }
}
