// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.StartupMemoryCheckConfig.ReportingLevel
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation

final case class StartupMemoryCheckConfig(reportingLevel: ReportingLevel)
    extends UniformCantonConfigValidation

object StartupMemoryCheckConfig {
  implicit val startupMemoryCheckConfigCantonConfigValidator
      : CantonConfigValidator[StartupMemoryCheckConfig] =
    CantonConfigValidatorDerivation[StartupMemoryCheckConfig]

  sealed trait ReportingLevel
  object ReportingLevel {
    implicit val reportingLevelCantonConfigValidator: CantonConfigValidator[ReportingLevel] =
      CantonConfigValidatorDerivation[ReportingLevel]

    final case object Warn extends ReportingLevel with UniformCantonConfigValidation

    final case object Ignore extends ReportingLevel with UniformCantonConfigValidation

    final case object Crash extends ReportingLevel with UniformCantonConfigValidation
  }
}
