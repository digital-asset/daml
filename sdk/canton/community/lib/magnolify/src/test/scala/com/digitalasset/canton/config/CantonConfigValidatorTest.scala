// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.config

import com.daml.nonempty.NonEmpty
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CantonConfigValidatorTest extends AnyWordSpec with Matchers {
  import CantonConfigValidatorTest.*

  "validate" in {
    val innerUniform = InnerUniform(1, "")
    innerUniform.validate(CommunityCantonEdition) shouldBe Right(())

    val innerEnterpriseOnly = InnerEnterpriseOnly("myString")
    val innerEnterpriseOnlyCommunityError = CantonConfigValidationError(
      s"Configuration ${innerEnterpriseOnly.getClass.getSimpleName} is supported only in $EnterpriseCantonEdition"
    )
    innerEnterpriseOnly.validate(CommunityCantonEdition) shouldBe
      Left(NonEmpty(Seq, innerEnterpriseOnlyCommunityError))
    innerEnterpriseOnly.validate(EnterpriseCantonEdition) shouldBe Right(())

    val innerCustom = InnerCustom(500, "myString")
    val innerCustomCommunityErrors = NonEmpty(
      Seq,
      CantonConfigValidationError(s"`a` must be less than 100 in $CommunityCantonEdition"),
      CantonConfigValidationError(s"`b` must be empty in $CommunityCantonEdition"),
    )
    innerCustom.validate(CommunityCantonEdition) shouldBe Left(innerCustomCommunityErrors)
    innerCustom.validate(EnterpriseCantonEdition) shouldBe Right(())

    val outerUniform = OuterUniform(
      innerUniform,
      innerEnterpriseOnly,
      Map("bad" -> innerCustom, "good" -> InnerCustom(0, "")),
    )
    val outerUniformCommunityErrors =
      innerEnterpriseOnlyCommunityError.augmentContext("innerEnterpriseOnly") +:
        innerCustomCommunityErrors.map(_.augmentContext("bad").augmentContext("innerCustoms"))
    outerUniform.validate(CommunityCantonEdition) shouldBe
      Left(outerUniformCommunityErrors)
    outerUniform.validate(EnterpriseCantonEdition) shouldBe
      Right(())

    val outerCustom = OuterCustom(Some(outerUniform), a = true)
    outerCustom.validate(CommunityCantonEdition) shouldBe
      Left(outerUniformCommunityErrors.map(_.augmentContext("nestedO")))
    outerCustom.validate(EnterpriseCantonEdition) shouldBe
      Left(NonEmpty(Seq, CantonConfigValidationError("complicated error condition")))

    val sealedTraitB: SealedTrait = SealedTraitVariantB("myString")
    val sealedTraitBEnterpriseError =
      s"Configuration ${sealedTraitB.getClass.getSimpleName} is supported only in $CommunityCantonEdition"
    sealedTraitB.validate(EnterpriseCantonEdition) shouldBe Left(
      NonEmpty(Seq, CantonConfigValidationError(sealedTraitBEnterpriseError))
    )
    sealedTraitB.validate(CommunityCantonEdition) shouldBe Right(())
  }

}

object CantonConfigValidatorTest {

  private final case class InnerUniform(a: Int, b: String) extends UniformCantonConfigValidation
  private object InnerUniform {
    implicit val validatorInnerUniform: CantonConfigValidator[InnerUniform] =
      manual.CantonConfigValidatorDerivation[InnerUniform]
  }

  private final case class InnerEnterpriseOnly(enterpriseConfig: String)
      extends EnterpriseOnlyCantonConfigValidation
  private object InnerEnterpriseOnly {
    implicit val validatorInnerEnterpriseOnly: CantonConfigValidator[InnerEnterpriseOnly] =
      manual.CantonConfigValidatorDerivation[InnerEnterpriseOnly]
  }

  private final case class InnerCustom(a: Int, b: String) extends CustomCantonConfigValidation {
    override protected def doValidate(edition: CantonEdition): Seq[CantonConfigValidationError] =
      edition match {
        case CommunityCantonEdition =>
          val as = Option.when(a >= 100)(
            CantonConfigValidationError(s"`a` must be less than 100 in $CommunityCantonEdition")
          )
          val bs = Option.when(b.nonEmpty)(
            CantonConfigValidationError(s"`b` must be empty in $CommunityCantonEdition")
          )
          Seq(as, bs).flatMap(_.toList)
        case EnterpriseCantonEdition =>
          Option
            .when(b.sizeCompare(a) > 0)(
              CantonConfigValidationError(
                s"`b` must be shorter than `a` in $EnterpriseCantonEdition"
              )
            )
            .toList
      }
  }
  private object InnerCustom {
    implicit val validatorInnerCustom: CantonConfigValidator[InnerCustom] =
      manual.CantonConfigValidatorDerivation[InnerCustom]
  }

  private final case class OuterUniform(
      innerUniform: InnerUniform,
      innerEnterpriseOnly: InnerEnterpriseOnly,
      innerCustoms: Map[String, InnerCustom],
  ) extends UniformCantonConfigValidation
  private object OuterUniform {
    implicit val validatorOuterUniform: CantonConfigValidator[OuterUniform] =
      manual.CantonConfigValidatorDerivation[OuterUniform]
  }

  private final case class OuterCustom(
      nestedO: Option[OuterUniform],
      a: Boolean,
  ) extends CustomCantonConfigValidation {
    override protected def doValidate(edition: CantonEdition): Seq[CantonConfigValidationError] =
      Option
        .when((a == nestedO.isEmpty) == (edition == CommunityCantonEdition))(
          CantonConfigValidationError("complicated error condition")
        )
        .toList
  }
  private object OuterCustom {
    implicit val validatorOuterCustom: CantonConfigValidator[OuterCustom] =
      manual.CantonConfigValidatorDerivation[OuterCustom]
  }

  private sealed trait SealedTrait extends CantonConfigValidation
  private object SealedTrait {
    implicit val validatorSealedTrait: CantonConfigValidator[SealedTrait] =
      manual.CantonConfigValidatorDerivation[SealedTrait]
  }
  private final case class SealedTraitVariantA(i: Int)
      extends SealedTrait
      with UniformCantonConfigValidation
  private final case class SealedTraitVariantB(s: String)
      extends SealedTrait
      with CommunityOnlyCantonConfigValidation
  private final case class SealedTraitVariantC(b: Boolean)
      extends SealedTrait
      with EnterpriseOnlyCantonConfigValidation
}
