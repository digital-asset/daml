// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.config.CantonConfigValidator.validateAll
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{
  CantonConfigValidator,
  PositiveFiniteDuration,
  UniformCantonConfigValidation,
}

final case class ProgressSupervisorConfig(
    enabled: Boolean = true,
    stuckDetectionTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofMinutes(15),
    logAtDebugLevelDuration: PositiveFiniteDuration = PositiveFiniteDuration.ofMinutes(5),
    warnAction: ProgressSupervisorConfig.WarnAction = ProgressSupervisorConfig.EnableDebugLogging,
) extends UniformCantonConfigValidation

object ProgressSupervisorConfig {

  sealed trait WarnAction extends Product with Serializable
  case object EnableDebugLogging extends WarnAction
  case object RestartSequencer extends WarnAction

  implicit val warnActionCantonConfigValidator: CantonConfigValidator[WarnAction] = validateAll

  implicit val progressSupervisorConfigCantonConfigValidator
      : CantonConfigValidator[ProgressSupervisorConfig] =
    CantonConfigValidatorDerivation[ProgressSupervisorConfig]
}
