// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.config.ConfigErrors.CantonConfigError
import com.digitalasset.canton.config.{CantonConfig, DefaultPorts}
import com.digitalasset.canton.environment.{CommunityEnvironmentFactory, EnvironmentFactory}
import com.typesafe.config.Config

object CantonCommunityApp extends CantonAppDriver {

  override def loadConfig(
      config: Config,
      defaultPorts: Option[DefaultPorts],
  ): Either[CantonConfigError, CantonConfig] =
    CantonConfig.loadAndValidate(config, defaultPorts)

  override protected def environmentFactory: EnvironmentFactory =
    CommunityEnvironmentFactory
}
