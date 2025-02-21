// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.config

import scala.language.experimental.macros

package object auto {

  /** Automatic derivation of [[CantonConfigValidator]] instances. When this implicit is in scope
    * when a [[CantonConfigValidator]] instance is summoned, e.g., via `implicitly` or
    * [[CantonConfigValidator.apply]], type class derivation will attempt to generate code for the
    * derivation on the spot, similar to [[semiauto.CantonConfigValidatorDerivation.apply]].
    *
    * AVOID USING THIS IMPLICIT UNLESS YOU ABSOLUTELY KNOW WHAT YOU'RE DOING. Sloppy usage can lead
    * to exponential compilation time and code size.
    */
  implicit def genCantonConfigValidator[T]: CantonConfigValidator[T] =
    macro CantonConfigValidationMacros.genCantonConfigValidatorMacro[T]
}
