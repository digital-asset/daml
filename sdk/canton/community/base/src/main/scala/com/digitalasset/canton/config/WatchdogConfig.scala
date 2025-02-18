// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation

import scala.concurrent.duration.*

final case class WatchdogConfig(
    enabled: Boolean,
    checkInterval: PositiveFiniteDuration = PositiveFiniteDuration(15.seconds),
    killDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration(30.seconds),
) extends UniformCantonConfigValidation

object WatchdogConfig {
  implicit val watchdogConfigCantonConfigValidator: CantonConfigValidator[WatchdogConfig] =
    CantonConfigValidatorDerivation[WatchdogConfig]
}
