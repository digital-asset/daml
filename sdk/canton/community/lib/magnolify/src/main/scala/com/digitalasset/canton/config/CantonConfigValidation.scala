// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.config

import com.daml.nonempty.NonEmpty

/** Trait for Canton config classes that can be validated for a [[CantonEdition]]. */
trait CantonConfigValidation {
  final def validate[T >: this.type](edition: CantonEdition)(implicit
      validator: CantonConfigValidator[T]
  ): Either[NonEmpty[Seq[CantonConfigValidationError]], Unit] =
    NonEmpty.from(validator.validate(edition, this).toVector).toLeft(())
}

/** Trait for Canton configuration classes that validate subconfigurations
  * recursively via Magnolia's derivation of the [[CantonConfigValidator]] type class.
  *
  * Validations specific to the given class should be done in `doValidate`.
  */
trait CustomCantonConfigValidation extends CantonConfigValidation {

  /** Returns all validation errors that are specific to this Canton configuration class.
    * Successful validation should return an empty sequence.
    *
    * Validation errors of subconfigurations should not be reported by this method,
    * but via the type class derivation.
    */
  protected def doValidate(edition: CantonEdition): Seq[CantonConfigValidationError]

  private[config] final def doValidateInternal(
      edition: CantonEdition
  ): Seq[CantonConfigValidationError] = doValidate(edition)
}

/** Trait for Canton config classes that do not impose any constraints on editions by themselves.
  * Subconfigurations may still distinguish between editions though.
  */
trait UniformCantonConfigValidation extends CustomCantonConfigValidation {
  override protected final def doValidate(
      edition: CantonEdition
  ): Seq[CantonConfigValidationError] = Seq.empty
}

/** Trait for Canton config classes that may only be used with the [[EnterpriseCantonEdition]].
  * Does not perform additional validation except for the subconfigurations' own validations.
  */
trait EnterpriseOnlyCantonConfigValidation extends CustomCantonConfigValidation {
  override protected final def doValidate(
      edition: CantonEdition
  ): Seq[CantonConfigValidationError] =
    Option
      .when(edition != EnterpriseCantonEdition)(
        CantonConfigValidationError(
          s"Configuration ${this.getClass.getSimpleName} is supported only in $EnterpriseCantonEdition"
        )
      )
      .toList
}

/** Trait for Canton config classes that may only be used with the [[CommunityCantonEdition]].
  * Does not perform additional validation except for the subconfigurations' own validations.
  */
trait CommunityOnlyCantonConfigValidation extends CustomCantonConfigValidation {
  override protected final def doValidate(
      edition: CantonEdition
  ): Seq[CantonConfigValidationError] =
    Option
      .when(edition != CommunityCantonEdition)(
        CantonConfigValidationError(
          s"Configuration ${this.getClass.getSimpleName} is supported only in $CommunityCantonEdition"
        )
      )
      .toList
}
