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
          "foo.bar",
          "foo!bar",
          "..",
        )
      )
      .value shouldBe Seq(
      UpdatePath(List("foo", "bar")),
      UpdatePath(List("foo")),
      UpdatePath(List("foo", "bar")),
      UpdatePath(List("foo!bar")),
      UpdatePath(List()),
    )
  }

}
