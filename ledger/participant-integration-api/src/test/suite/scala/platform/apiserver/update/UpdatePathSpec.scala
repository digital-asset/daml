// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

import org.scalatest.EitherValues
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class UpdatePathSpec extends AnyFreeSpec with Matchers with EitherValues {

  "parse valid update paths" in {
    UpdatePath
      .parseAll(
        Seq(
          "foo.bar",
          "foo",
          "foo.bar!merge",
          "foo.bar!replace",
        )
      )
      .value shouldBe Seq(
      UpdatePath(List("foo", "bar"), UpdatePathModifier.NoModifier),
      UpdatePath(List("foo"), UpdatePathModifier.NoModifier),
      UpdatePath(List("foo", "bar"), UpdatePathModifier.Merge),
      UpdatePath(List("foo", "bar"), UpdatePathModifier.Replace),
    )
  }

  "raise errors when parsing invalid paths" in {
    UpdatePath.parseAll(Seq("")).left.value shouldBe UpdatePathError.EmptyFieldPath("")
    UpdatePath.parseAll(Seq("!merge")).left.value shouldBe UpdatePathError.EmptyFieldPath("!merge")
    UpdatePath.parseAll(Seq("!replace")).left.value shouldBe UpdatePathError.EmptyFieldPath(
      "!replace"
    )
    UpdatePath.parseAll(Seq("!bad")).left.value shouldBe UpdatePathError.EmptyFieldPath("!bad")
    UpdatePath.parseAll(Seq("foo!bad")).left.value shouldBe UpdatePathError.UnknownUpdateModifier(
      "foo!bad"
    )
  }

}
