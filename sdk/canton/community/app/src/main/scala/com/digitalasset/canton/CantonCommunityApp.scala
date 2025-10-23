// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.config.ConfigErrors.CantonConfigError
import com.digitalasset.canton.config.{CantonConfig, CommunityCantonEdition, DefaultPorts}
import com.digitalasset.canton.environment.{CommunityEnvironmentFactory, EnvironmentFactory}
import com.typesafe.config.Config

object CantonCommunityApp extends CantonAppDriver {

  override def loadConfig(
      config: Config,
      defaultPorts: Option[DefaultPorts],
  ): Either[CantonConfigError, CantonConfig] =
    CantonConfig.loadAndValidate(config, CommunityCantonEdition, defaultPorts)

  override protected def environmentFactory: EnvironmentFactory =
    CommunityEnvironmentFactory
}
