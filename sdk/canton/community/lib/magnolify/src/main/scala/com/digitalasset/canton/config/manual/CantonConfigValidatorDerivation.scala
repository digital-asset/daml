// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.config.manual

import cats.data.Chain
import com.digitalasset.canton.config.{
  CantonConfigPrevalidator,
  CantonConfigValidationError,
  CantonConfigValidator,
  CantonEdition,
}
import magnolia1.{Magnolia, ReadOnlyCaseClass, SealedTrait}

import scala.language.experimental.macros

trait CantonConfigValidatorDerivation {
  type Typeclass[A] = CantonConfigValidator[A]

  def join[A](
      ctx: ReadOnlyCaseClass[Typeclass, A]
  )(implicit prevalidator: CantonConfigPrevalidator[A]): Typeclass[A] =
    new CantonConfigValidator[A] {
      override def validate(edition: CantonEdition, config: A): Chain[CantonConfigValidationError] =
        Chain.fromSeq(prevalidator.prevalidate(edition, config)) ++
          Chain.fromSeq(ctx.parameters).flatMap { param =>
            param.typeclass
              .validate(edition, param.dereference(config))
              .map(_.augmentContext(param.label))
          }
    }

  def split[A](ctx: SealedTrait[Typeclass, A]): Typeclass[A] =
    new CantonConfigValidator[A] {
      override def validate(edition: CantonEdition, config: A): Chain[CantonConfigValidationError] =
        ctx.split(config)(subtype => subtype.typeclass.validate(edition, subtype.cast(config)))
    }
}

object CantonConfigValidatorDerivation extends CantonConfigValidatorDerivation {

  /** Manual invocation for deriving a [[CantonConfigValidator]] instance from a
    * [[CantonConfigPrevalidator]] instance (found by implicit resolution). Implicit resolution must
    * also find [[CantonConfigValidator]] instances for all subconfigurations of `A`.
    *
    * In particular, if `A` is a case class, then implicit resolution must find
    * [[CantonConfigValidator]] instances for all the fields of the case class. It does not
    * automatically attempt to derive such instances. If `A` is a sealed trait of such case classes,
    * then this applies analogously to each implementing case class.
    *
    * Manual is the preferred approach to deriving [[CantonConfigValidator]] instances for the
    * following reasons:
    *   - It is more predictable than
    *     [[com.digitalasset.canton.config.semiauto.CantonConfigValidatorDerivation semi-automatic]]
    *     and automatic derivation. In particular, error messages about missing implicits for
    *     [[CantonConfigPrevalidator]] or [[CantonConfigValidator]] instances are more precise.
    *   - If the derived instances are systematically bound to implicit `val`s in scope, we will
    *     generate the code for the type class instance only once in the whole codebase, whereas the
    *     other approaches may generate the code once per usage of a configuration as a
    *     subconfiguration of other configurations. This increases compile time.
    */
  def apply[A]: CantonConfigValidator[A] = macro Magnolia.gen[A]
}
