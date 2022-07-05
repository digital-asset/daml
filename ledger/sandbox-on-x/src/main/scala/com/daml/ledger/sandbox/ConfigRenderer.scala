// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.ledger.sandbox

import com.typesafe.config.{ConfigRenderOptions, ConfigValue}

object ConfigRenderer {
  val renderOptions: ConfigRenderOptions = ConfigRenderOptions
    .defaults()
    .setOriginComments(false)
    .setComments(false)
    .setJson(false)
    .setFormatted(true)

  private def toConfig(
      config: SandboxOnXConfig
  )(implicit writer: pureconfig.ConfigWriter[SandboxOnXConfig]): ConfigValue =
    writer.to(config)

  def render(configValue: ConfigValue): String =
    configValue.render(renderOptions)

  def render(
      config: SandboxOnXConfig
  )(implicit writer: pureconfig.ConfigWriter[SandboxOnXConfig]): String = {
    val configValue = toConfig(config)
    render(configValue)
  }

}
