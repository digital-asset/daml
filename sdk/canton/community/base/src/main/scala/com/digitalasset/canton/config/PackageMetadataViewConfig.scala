// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.PackageMetadataViewConfig.{
  DefaultInitLoadParallelism,
  DefaultInitProcessParallelism,
  DefaultInitTakesTooLongInitialDelay,
  DefaultInitTakesTooLongInterval,
}
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation

import scala.concurrent.duration.{FiniteDuration, *}

final case class PackageMetadataViewConfig(
    initLoadParallelism: Int = DefaultInitLoadParallelism,
    initProcessParallelism: Int = DefaultInitProcessParallelism,
    initTakesTooLongInitialDelay: FiniteDuration = DefaultInitTakesTooLongInitialDelay,
    initTakesTooLongInterval: FiniteDuration = DefaultInitTakesTooLongInterval,
) extends UniformCantonConfigValidation

object PackageMetadataViewConfig {
  implicit val packageMetadataViewConfigCantonConfigValidator
      : CantonConfigValidator[PackageMetadataViewConfig] =
    CantonConfigValidatorDerivation[PackageMetadataViewConfig]

  val DefaultInitLoadParallelism: Int = 16
  val DefaultInitProcessParallelism: Int = 16
  val DefaultInitTakesTooLongInitialDelay: FiniteDuration = 1.minute
  val DefaultInitTakesTooLongInterval: FiniteDuration = 10.seconds
}
