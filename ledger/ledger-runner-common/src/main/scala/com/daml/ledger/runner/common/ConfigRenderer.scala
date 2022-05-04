// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.typesafe.config.{ConfigRenderOptions, ConfigValue}

object ConfigRenderer {
  val renderOptions: ConfigRenderOptions = ConfigRenderOptions
    .defaults()
    .setOriginComments(false)
    .setComments(false)
    .setJson(false)
    .setFormatted(true)

  private def toConfig(
      config: Config
  )(implicit writer: pureconfig.ConfigWriter[Config]): ConfigValue =
    writer.to(config)

  def render(configValue: ConfigValue): String =
    configValue.render(renderOptions)

  def render(config: Config): String = {
    import PureConfigReaderWriter._
    val configValue = toConfig(config)
    render(configValue)
  }

}
