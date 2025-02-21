// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.config

import cats.data.Chain
import com.daml.nonempty.{NonEmpty, NonEmptyColl}

import java.io.File
import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration

/** Type class for validations of Canton config classes depending on [[CantonEdition]]s. Instances
  * are typically derived using [[manual.CantonConfigValidatorDerivation]] via Magnolia so that
  * validation automatically calls `validate` on the subconfigurations, where the subconfigurations
  * of `A` are all the types of fields of a case class that implements `A`. The derivation must find
  * a corresponding [[CantonConfigPrevalidator]] instance via implicit resolution.
  *
  * @tparam A
  *   the type of the config class to validate. This should normally be contravariant, but
  *   Magnolia's derivation algorithm cannot deal with contravariant type classes for sealed traits.
  */
trait CantonConfigValidator[A] {
  def validate(
      edition: CantonEdition,
      config: A,
  ): Chain[CantonConfigValidationError]
}

object CantonConfigValidator {

  /** Summons an instance of the type class [[CantonConfigValidator]].
    *
    * This triggers automatic derivation at the call site in the scope of `import
    * com.digitalasset.canton.config.auto.*`.
    */
  @inline def apply[A](implicit ev: CantonConfigValidator[A]): CantonConfigValidator[A] = ev

  /** Validation instance that accepts all values of the given type for any edition. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def validateAll[A]: CantonConfigValidator[A] =
    // CantonConfigValidator should be contravariant, but cannot be due to Magnolia's limitations.
    // So we have to cast this explicitly here.
    validateAnything.asInstanceOf[CantonConfigValidator[A]]
  private val validateAnything: CantonConfigValidator[Any] =
    new CantonConfigValidator[Any] {
      override def validate(
          edition: CantonEdition,
          config: Any,
      ): Chain[CantonConfigValidationError] =
        Chain.empty
    }

  implicit def cantonConfigValidatorChar: CantonConfigValidator[Char] = validateAll
  implicit def cantonConfigValidatorString: CantonConfigValidator[String] = validateAll
  implicit def cantonConfigValidatorByte: CantonConfigValidator[Byte] = validateAll
  implicit def cantonConfigValidatorShort: CantonConfigValidator[Short] = validateAll
  implicit def cantonConfigValidatorInt: CantonConfigValidator[Int] = validateAll
  implicit def cantonConfigValidatorLong: CantonConfigValidator[Long] = validateAll
  implicit def cantonConfigValidatorFloat: CantonConfigValidator[Float] = validateAll
  implicit def cantonConfigValidatorDouble: CantonConfigValidator[Double] = validateAll
  implicit def cantonConfigValidatorBoolean: CantonConfigValidator[Boolean] = validateAll

  def validateWithoutRecursion[T: CantonConfigPrevalidator]: CantonConfigValidator[T] =
    new CantonConfigValidator[T] {
      override def validate(
          edition: CantonEdition,
          config: T,
      ): Chain[CantonConfigValidationError] =
        Chain.fromSeq(implicitly[CantonConfigPrevalidator[T]].prevalidate(edition, config))

    }

  implicit def cantonConfigValidatorMap[K, A](implicit
      ev: CantonConfigValidator[A]
  ): CantonConfigValidator[Map[K, A]] = new CantonConfigValidator[Map[K, A]] {
    override def validate(
        edition: CantonEdition,
        config: Map[K, A],
    ): Chain[CantonConfigValidationError] =
      config.iterator.foldLeft(Chain.empty[CantonConfigValidationError]) { (acc, kv) =>
        val (key, value) = kv
        acc ++ ev.validate(edition, value).map(_.augmentContext(key.toString))
      }
  }

  implicit def cantonConfigValidatorOption[A](implicit
      ev: CantonConfigValidator[A]
  ): CantonConfigValidator[Option[A]] = new CantonConfigValidator[Option[A]] {
    override def validate(
        edition: CantonEdition,
        config: Option[A],
    ): Chain[CantonConfigValidationError] = config match {
      case None => Chain.empty
      case Some(a) => ev.validate(edition, a)
    }
  }

  implicit def cantonConfigValidatorNonEmpty[A](implicit
      ev: CantonConfigValidator[A]
  ): CantonConfigValidator[NonEmpty[A]] = new CantonConfigValidator[NonEmpty[A]] {
    override def validate(
        edition: CantonEdition,
        config: NonEmpty[A],
    ): Chain[CantonConfigValidationError] =
      ev.validate(edition, NonEmptyColl.widen(config))
  }

  implicit def cantonConfigValidatorSet[A](implicit
      ev: CantonConfigValidator[A]
  ): CantonConfigValidator[Set[A]] = new CantonConfigValidator[Set[A]] {
    override def validate(
        edition: CantonEdition,
        config: Set[A],
    ): Chain[CantonConfigValidationError] =
      config.iterator.foldLeft(Chain.empty[CantonConfigValidationError]) { (acc, a) =>
        acc ++ ev.validate(edition, a)
      }
  }

  implicit def cantonConfigValidatorSeq[A](implicit
      ev: CantonConfigValidator[A]
  ): CantonConfigValidator[Seq[A]] = new CantonConfigValidator[Seq[A]] {
    override def validate(
        edition: CantonEdition,
        config: Seq[A],
    ): Chain[CantonConfigValidationError] =
      config.iterator.foldLeft(Chain.empty[CantonConfigValidationError]) { (acc, a) =>
        acc ++ ev.validate(edition, a)
      }
  }

  implicit val cantonConfigValidatorFile: CantonConfigValidator[File] = validateAll
  implicit val cantonConfigValidatorPath: CantonConfigValidator[Path] = validateAll
  implicit val cantonConfigValidatorFiniteDuration: CantonConfigValidator[FiniteDuration] =
    validateAll
}
