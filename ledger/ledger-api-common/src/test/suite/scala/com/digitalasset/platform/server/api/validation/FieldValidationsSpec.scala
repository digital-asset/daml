// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.platform.server.api.validation.FieldValidations.{
  ExceededAnnotationsSizeError,
  InvalidAnnotationsKeySyntaxError,
  verifyMetadataAnnotations2,
}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FieldValidationsSpec extends AnyFlatSpec with Matchers with EitherValues {

  it should "validate valid annotations key names" in {
    // invalid characters
    verifyMetadataAnnotations2(Map("" -> "")) shouldBe Left(InvalidAnnotationsKeySyntaxError())
    verifyMetadataAnnotations2(Map("&" -> "")) shouldBe Left(InvalidAnnotationsKeySyntaxError())
    verifyMetadataAnnotations2(Map("%" -> "")) shouldBe Left(InvalidAnnotationsKeySyntaxError())
    // valid characters
    verifyMetadataAnnotations2(Map("a" -> "")) shouldBe Right(())
    verifyMetadataAnnotations2(Map("Z" -> "")) shouldBe Right(())
    verifyMetadataAnnotations2(Map("7" -> "")) shouldBe Right(())
    verifyMetadataAnnotations2(Map("aA._-b" -> "")) shouldBe Right(())
    verifyMetadataAnnotations2(Map("aA._-b" -> "")) shouldBe Right(())
    verifyMetadataAnnotations2(
      Map("abcdefghijklmnopqrstuvwxyz.-_ABCDEFGHIJKLMNOPQRSTUVWXYZ" -> "")
    ) shouldBe Right(())
    verifyMetadataAnnotations2(Map("some-key.like_this" -> "")) shouldBe Right(())
    // too long
    verifyMetadataAnnotations2(Map("a" * 64 -> "")) shouldBe Left(
      InvalidAnnotationsKeySyntaxError()
    )
    // just right size
    verifyMetadataAnnotations2(Map("a" * 63 -> "")) shouldBe Right(())
    // character in an invalid position
    verifyMetadataAnnotations2(Map(".aaa" -> "")) shouldBe Left(InvalidAnnotationsKeySyntaxError())
    verifyMetadataAnnotations2(Map("aaa_" -> "")) shouldBe Left(InvalidAnnotationsKeySyntaxError())
    verifyMetadataAnnotations2(Map("aaa-" -> "")) shouldBe Left(InvalidAnnotationsKeySyntaxError())
  }

  it should "validate valid annotations keys prefixes" in {
    verifyMetadataAnnotations2(Map("aaa/a" -> "")) shouldBe Right(())
    verifyMetadataAnnotations2(Map("AAA/a" -> "")) shouldBe Right(())
    verifyMetadataAnnotations2(Map("aaa-bbb/a" -> "")) shouldBe Right(())
    verifyMetadataAnnotations2(Map("aaa-bbb.ccc-ddd/a" -> "")) shouldBe Right(())
    verifyMetadataAnnotations2(Map("00.11.AA-bBb.ccc2/a" -> "")) shouldBe Right(())
    verifyMetadataAnnotations2(Map("aa--aa/a" -> "")) shouldBe Right(())

    verifyMetadataAnnotations2(Map(".user.management.daml/foo_" -> "")) shouldBe Left(
      InvalidAnnotationsKeySyntaxError()
    )
    verifyMetadataAnnotations2(Map("aaa./a" -> "")) shouldBe Left(
      InvalidAnnotationsKeySyntaxError()
    )
    verifyMetadataAnnotations2(Map(".aaa/a" -> "")) shouldBe Left(
      InvalidAnnotationsKeySyntaxError()
    )
    verifyMetadataAnnotations2(Map("-aaa/a" -> "")) shouldBe Left(
      InvalidAnnotationsKeySyntaxError()
    )
    verifyMetadataAnnotations2(Map("aaa-/a" -> "")) shouldBe Left(
      InvalidAnnotationsKeySyntaxError()
    )
    verifyMetadataAnnotations2(Map("aa..aa/a" -> "")) shouldBe Left(
      InvalidAnnotationsKeySyntaxError()
    )

    verifyMetadataAnnotations2(Map(s"${"a" * 254}/a" -> "")) shouldBe Left(
      InvalidAnnotationsKeySyntaxError()
    )
    verifyMetadataAnnotations2(Map(s"${"a" * 253}/a" -> "")) shouldBe Right(())
  }

  it should "validate annotations' total size - single key, large value" in {
    val largeString = "a" * 256 * 1024
    val notSoLargeString = "a" * ((256 * 1024) - 1)

    verifyMetadataAnnotations2(annotations = Map("a" -> largeString)) shouldBe Left(
      ExceededAnnotationsSizeError((256 * 1024) + 1)
    )
    verifyMetadataAnnotations2(annotations = Map("a" -> notSoLargeString)) shouldBe Right(())
  }

  it should "validate annotations' total size - many keys" in {
    val sixteenLetters = "abcdefghijklmnop"
    val value = "a" * 1022
    val mapWithManyKeys: Map[String, String] = (for {
      l1 <- sixteenLetters
      l2 <- sixteenLetters
    } yield {
      val key = s"$l1$l2"
      key -> value
    }).toMap
    val mapWithManyKeys2 = mapWithManyKeys.updated(key = "a", value = "")

    verifyMetadataAnnotations2(annotations = mapWithManyKeys) shouldBe Right(())
    verifyMetadataAnnotations2(annotations = mapWithManyKeys2) shouldBe Left(
      ExceededAnnotationsSizeError((256 * 1024) + 1)
    )
  }

}
