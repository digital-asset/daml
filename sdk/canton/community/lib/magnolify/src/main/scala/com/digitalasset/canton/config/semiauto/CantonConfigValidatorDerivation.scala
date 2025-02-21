// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.config.semiauto

import com.digitalasset.canton.config.CantonConfigValidator
import magnolia1.Magnolia

import scala.language.experimental.macros

object CantonConfigValidatorDerivation
    extends com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation {

  /** Manual invocation for deriving a [[CantonConfigValidator]] instance from a
    * [[CantonConfigPrevalidator]] instance (found by implicit resolution) for the configuration `A`
    * and all its subconfigurations unless implicit resolution already finds a
    * [[CantonConfigValidator]] instance for them. Implicit resolution must also find
    * [[CantonConfigPrevalidator]] instances for all subconfigurations of `A` for which derivation
    * happens.
    *
    * [[manual.CantonConfigValidatorDerivation]] should be preferred when possible because it is
    * more predictable and usually more efficient. In particular, derivation failures can be hard to
    * debug for semi-automatic derivation. For example, suppose that `A` is a case class `Foo` with
    * a field `i: Option[Bar]` where implicit resolution does not find a [[CantonConfigValidator]]
    * instance for `Bar`. The derivation sees that `Option` is a sealed trait of case classes `None`
    * and `Some` and therefore attempts to derive a [[CantonConfigValidator]] instance for `None`
    * and `Some`. The derivation for `None` will fail though because there is no
    * [[CantonConfigPrevalidator]] instance for `None`. This makes the overall derivation fail with
    * a rather obscure error message about implicit resolution problems.
    */
  implicit def apply[A]: CantonConfigValidator[A] = macro Magnolia.gen[A]
}
