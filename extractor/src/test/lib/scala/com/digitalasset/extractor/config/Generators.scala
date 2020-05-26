// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.config

import org.scalacheck.{Arbitrary, Gen}
import scalaz.OneAnd
import scalaz.Scalaz._

object Generators {

  implicit def arbTemplateConfig: Arbitrary[TemplateConfig] = Arbitrary(genTemplateConfig)

  def genTemplateConfig: Gen[TemplateConfig] =
    for {
      moduleName <- Gen.identifier
      entityName <- Gen.identifier
    } yield TemplateConfig(moduleName, entityName)

  def templateConfigUserInput(templateConfig: TemplateConfig): String =
    templateConfig.moduleName + ':'.toString + templateConfig.entityName

  def templateConfigUserInput(templateConfigs: OneAnd[List, TemplateConfig]): String =
    templateConfigUserInput(templateConfigs.toList)

  def templateConfigUserInput(templateConfigs: List[TemplateConfig]): String =
    templateConfigs.map(templateConfigUserInput).mkString(",")
}
