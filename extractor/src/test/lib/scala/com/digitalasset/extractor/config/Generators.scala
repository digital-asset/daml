// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.config

import org.scalacheck.Gen

object Generators {

  def genTemplateConfig: Gen[TemplateConfig] =
    for {
      moduleName <- Gen.identifier
      entityName <- Gen.identifier
    } yield TemplateConfig(moduleName, entityName)

  def templateConfigUserInput(templateConfig: TemplateConfig): String =
    templateConfig.moduleName + ':'.toString + templateConfig.entityName

  def templateConfigUserInput(templateConfigs: List[TemplateConfig]): String =
    templateConfigs.map(templateConfigUserInput).mkString(",")
}
