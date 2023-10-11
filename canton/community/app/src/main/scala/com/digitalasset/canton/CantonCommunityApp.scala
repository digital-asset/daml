// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.config.CantonCommunityConfig
import com.digitalasset.canton.config.ConfigErrors.CantonConfigError
import com.digitalasset.canton.environment.{
  CommunityEnvironment,
  CommunityEnvironmentFactory,
  EnvironmentFactory,
}
import com.typesafe.config.Config

object CantonCommunityApp extends CantonAppDriver[CommunityEnvironment] {

  override def loadConfig(config: Config): Either[CantonConfigError, CantonCommunityConfig] =
    CantonCommunityConfig.load(config)

  override protected def environmentFactory: EnvironmentFactory[CommunityEnvironment] =
    CommunityEnvironmentFactory

  override protected def withManualStart(config: CantonCommunityConfig): CantonCommunityConfig =
    config.copy(parameters = config.parameters.copy(manualStart = true))
}
