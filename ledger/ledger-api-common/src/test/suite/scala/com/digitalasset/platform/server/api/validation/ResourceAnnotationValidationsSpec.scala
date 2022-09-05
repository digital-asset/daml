// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.ledger.participant.state.index.ResourceAnnotationValidation._
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ResourceAnnotationValidationsSpec extends AnyFlatSpec with Matchers with EitherValues {

  it should "validate valid annotations key names" in {
    // invalid characters
    validateAnnotations(Map("" -> "")).left.value shouldBe a[InvalidAnnotationsKeyError]
    validateAnnotations(Map("&" -> "")).left.value shouldBe a[InvalidAnnotationsKeyError]
    validateAnnotations(Map("%" -> "")).left.value shouldBe a[InvalidAnnotationsKeyError]
    // valid characters
    validateAnnotations(Map("a" -> "")).value shouldBe ()
    validateAnnotations(Map("Z" -> "")).value shouldBe ()
    validateAnnotations(Map("7" -> "")).value shouldBe ()
    validateAnnotations(Map("aA._-b" -> "")).value shouldBe ()
    validateAnnotations(Map("aA._-b" -> "")).value shouldBe ()
    validateAnnotations(
      Map("abcdefghijklmnopqrstuvwxyz.-_ABCDEFGHIJKLMNOPQRSTUVWXYZ" -> "")
    ) shouldBe Right(())
    validateAnnotations(Map("some-key.like_this" -> "")).value shouldBe ()
    // too long
    validateAnnotations(Map("a" * 64 -> "")).left.value shouldBe a[InvalidAnnotationsKeyError]
    // just right size
    validateAnnotations(Map("a" * 63 -> "")).value shouldBe ()
    // character in an invalid position
    validateAnnotations(Map(".aaa" -> "")).left.value shouldBe a[InvalidAnnotationsKeyError]
    validateAnnotations(Map("aaa_" -> "")).left.value shouldBe a[InvalidAnnotationsKeyError]
    validateAnnotations(Map("aaa-" -> "")).left.value shouldBe a[InvalidAnnotationsKeyError]
  }

  it should "validate valid annotations keys prefixes" in {
    validateAnnotations(Map("aaa/a" -> "")).value shouldBe ()
    validateAnnotations(Map("AAA/a" -> "")).value shouldBe ()
    validateAnnotations(Map("aaa-bbb/a" -> "")).value shouldBe ()
    validateAnnotations(Map("aaa-bbb.ccc-ddd/a" -> "")).value shouldBe ()
    validateAnnotations(Map("00.11.AA-bBb.ccc2/a" -> "")).value shouldBe ()
    validateAnnotations(Map("aa--aa/a" -> "")).value shouldBe ()

    validateAnnotations(Map(".user.management.daml/foo_" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    validateAnnotations(Map("aaa./a" -> "")).left.value shouldBe a[InvalidAnnotationsKeyError]
    validateAnnotations(Map(".aaa/a" -> "")).left.value shouldBe a[InvalidAnnotationsKeyError]
    validateAnnotations(Map("-aaa/a" -> "")).left.value shouldBe a[InvalidAnnotationsKeyError]
    validateAnnotations(Map("aaa-/a" -> "")).left.value shouldBe a[InvalidAnnotationsKeyError]
    validateAnnotations(Map("aa..aa/a" -> "")).left.value shouldBe a[InvalidAnnotationsKeyError]

    validateAnnotations(Map(s"${"a" * 254}/a" -> "")).left.value shouldBe a[
      InvalidAnnotationsKeyError
    ]
    validateAnnotations(Map(s"${"a" * 253}/a" -> "")).value shouldBe ()
  }

  it should "validate annotations' total size - single key, large value" in {
    val largeString = "a" * 256 * 1024
    val notSoLargeString = "a" * ((256 * 1024) - 1)

    validateAnnotations(annotations =
      Map("a" -> largeString)
    ).left.value shouldBe AnnotationsSizeExceededError
    validateAnnotations(annotations = Map("a" -> notSoLargeString)).value shouldBe ()
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
    val mapWithManyKeys2 = mapWithManyKeys.updated(key = "a", value = "")

    validateAnnotations(annotations = mapWithManyKeys).value shouldBe ()
    validateAnnotations(annotations =
      mapWithManyKeys2
    ).left.value shouldBe AnnotationsSizeExceededError
  }

}
