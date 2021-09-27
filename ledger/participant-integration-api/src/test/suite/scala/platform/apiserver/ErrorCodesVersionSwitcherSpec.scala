// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ErrorCodesVersionSwitcherSpec extends AnyFlatSpec with Matchers {

  behavior of classOf[ErrorCodesVersionSwitcher].getSimpleName

  it should "use v2 error codes" in {
    // given
    val tested = ErrorCodesVersionSwitcher(enableErrorCodesV2 = true)
    val expected = new StatusRuntimeException(Status.INTERNAL)

    // when
    val actual =
      tested.choose(v1 = fail("This argument should be evaluated lazily!"), v2 = expected)

    // then
    actual shouldBe expected
  }

  it should "use v1 error codes" in {
    // given
    val tested = ErrorCodesVersionSwitcher(enableErrorCodesV2 = false)
    val expected = new StatusRuntimeException(Status.INTERNAL)

    // when
    val actual =
      tested.choose(v1 = expected, v2 = fail("This argument should be evaluated lazily!"))

    // then
    actual shouldBe expected
  }

}
