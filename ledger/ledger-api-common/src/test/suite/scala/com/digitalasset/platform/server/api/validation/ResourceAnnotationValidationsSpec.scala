// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import ResourceAnnotationValidation._
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ResourceAnnotationValidationsSpec extends AnyFlatSpec with Matchers with EitherValues {

  private def testedValidationAllowEmptyValues(v: Map[String, String]) =
    validateAnnotationsFromApiRequest(v, allowEmptyValues = true)
  private def testedValidationDisallowEmptyValues(v: Map[String, String]) =
    validateAnnotationsFromApiRequest(v, allowEmptyValues = false)

  it should "validate annotation values" in {
    testedValidationDisallowEmptyValues(Map("a" -> "")).left.value shouldBe a[
      EmptyAnnotationsValueError
    ]
  }

  it should "validate annotations key names" in {
    // invalid characters
    testedValidationAllowEmptyValues(Map("" -> "a")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    testedValidationAllowEmptyValues(Map("&" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    testedValidationAllowEmptyValues(Map("%" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    // valid characters
    testedValidationAllowEmptyValues(Map("a" -> "")).value shouldBe ()
    testedValidationAllowEmptyValues(Map("Z" -> "")).value shouldBe ()
    testedValidationAllowEmptyValues(Map("7" -> "")).value shouldBe ()
    testedValidationAllowEmptyValues(Map("aA._-b" -> "")).value shouldBe ()
    testedValidationAllowEmptyValues(Map("aA._-b" -> "")).value shouldBe ()
    testedValidationAllowEmptyValues(
      Map("abcdefghijklmnopqrstuvwxyz.-_ABCDEFGHIJKLMNOPQRSTUVWXYZ" -> "")
    ) shouldBe Right(())
    testedValidationAllowEmptyValues(Map("some-key.like_this" -> "")).value shouldBe ()
    // too long
    testedValidationAllowEmptyValues(Map("a" * 64 -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    // just right size
    testedValidationAllowEmptyValues(Map("a" * 63 -> "")).value shouldBe ()
    // character in an invalid position
    testedValidationAllowEmptyValues(Map(".aaa" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    testedValidationAllowEmptyValues(Map("aaa_" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    testedValidationAllowEmptyValues(Map("aaa-" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
  }

  it should "validate annotations keys prefixes" in {
    testedValidationAllowEmptyValues(Map("aaa/a" -> "")).value shouldBe ()
    testedValidationAllowEmptyValues(Map("AAA/a" -> "")).value shouldBe ()
    testedValidationAllowEmptyValues(Map("aaa-bbb/a" -> "")).value shouldBe ()
    testedValidationAllowEmptyValues(Map("aaa-bbb.ccc-ddd/a" -> "")).value shouldBe ()
    testedValidationAllowEmptyValues(Map("00.11.AA-bBb.ccc2/a" -> "")).value shouldBe ()
    testedValidationAllowEmptyValues(Map("aa--aa/a" -> "")).value shouldBe ()
    testedValidationAllowEmptyValues(Map(".user.management.daml/foo_" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    testedValidationAllowEmptyValues(Map("aaa./a" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    testedValidationAllowEmptyValues(Map(".aaa/a" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    testedValidationAllowEmptyValues(Map("-aaa/a" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    testedValidationAllowEmptyValues(Map("aaa-/a" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    testedValidationAllowEmptyValues(Map("aa..aa/a" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    testedValidationAllowEmptyValues(Map(s"${"a" * 254}/a" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    testedValidationAllowEmptyValues(Map(s"${"a" * 253}/a" -> "")).value shouldBe ()
  }

  it should "validate annotations' total size - single key, large value" in {
    val largeString = "a" * 256 * 1024
    val notSoLargeString = "a" * ((256 * 1024) - 1)
    testedValidationAllowEmptyValues(
      Map("a" -> largeString)
    ).left.value shouldBe AnnotationsSizeExceededError
    testedValidationAllowEmptyValues(Map("a" -> notSoLargeString)).value shouldBe ()
  }

  it should "do not count keys with empty values towards size limit" in {
    val notSoLargeString = "a" * ((256 * 1024) - 1)
    testedValidationAllowEmptyValues(
      Map("a" -> notSoLargeString, "deleteMe" -> "")
    ).value shouldBe ()
    testedValidationAllowEmptyValues(
      Map("a" -> notSoLargeString, "deleteM" -> "e")
    ).left.value shouldBe AnnotationsSizeExceededError
  }

  it should "validate annotations' total size - multiple keys" in {
    val sixteenLetters = "abcdefghijklmnop"
    val value = "a" * 1022
    val mapWithManyKeys: Map[String, String] = (for {
      l1 <- sixteenLetters
      l2 <- sixteenLetters
    } yield {
      val key = s"$l1$l2"
      key -> value
    }).toMap
    val mapWithManyKeys2 = mapWithManyKeys.updated(key = "a", value = "b")
    testedValidationAllowEmptyValues(mapWithManyKeys).value shouldBe ()
    testedValidationAllowEmptyValues(
      mapWithManyKeys2
    ).left.value shouldBe AnnotationsSizeExceededError
  }

}
