// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{
  CantonConfigValidator,
  PositiveFiniteDuration,
  UniformCantonConfigValidation,
}

final case class ProgressSupervisorConfig(
    enabled: Boolean = true,
    stuckDetectionTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(30),
    logAtDebugLevelDuration: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(60),
) extends UniformCantonConfigValidation

object ProgressSupervisorConfig {
  implicit val progressSupervisorConfigCantonConfigValidator
      : CantonConfigValidator[ProgressSupervisorConfig] =
    CantonConfigValidatorDerivation[ProgressSupervisorConfig]
}
