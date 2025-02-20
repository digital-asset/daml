// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.config

/** Type class for the validation checks that are specific for a particular configuration class.
  * Unlike [[CantonConfigValidator]], the checks performed by instances of this type class do not
  * recurse into subconfigurations.
  *
  * The Magnolia type class derivation in [[manual.CantonConfigValidatorDerivation]] derives a
  * [[CantonConfigValidator]] instance from a [[CantonConfigPrevalidator]] instance, which will
  * recurse into the subconfigurations. To that end, the [[CantonConfigPrevalidator]] instance must
  * be found by implicit resolution where the derivation occurs.
  *
  * Typically, instances of this type class are obtained via
  * [[CantonConfigPrevalidator.customCantonConfigValidationPrevalidator]] for configuration classes
  * that implement [[CustomCantonConfigValidation]].
  *
  * @tparam A
  *   The configuration class to validate
  */
trait CantonConfigPrevalidator[-A] {

  /** Checks the configuration `config` for validity in the given [[CantonEdition]] and returns the
    * sequence of validation errors if any are found.
    *
    * This method should look into subconfigurations only to the extent that it performs checks that
    * are specific to the subconfiguration appearing in `config`. The subconfiguration's own
    * validation is handled by the [[CantonConfigValidator]] instance that Magnolia should derive on
    * top of this [[CantonConfigPrevalidator]] instance.
    */
  def prevalidate(edition: CantonEdition, config: A): Seq[CantonConfigValidationError]
}

object CantonConfigPrevalidator {
  implicit val customCantonConfigValidationPrevalidator
      : CantonConfigPrevalidator[CustomCantonConfigValidation] =
    new CantonConfigPrevalidator[CustomCantonConfigValidation] {
      override def prevalidate(
          edition: CantonEdition,
          config: CustomCantonConfigValidation,
      ): Seq[CantonConfigValidationError] =
        config.doValidateInternal(edition)
    }
}
