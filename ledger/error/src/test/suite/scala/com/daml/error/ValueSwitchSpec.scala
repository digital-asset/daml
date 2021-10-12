// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ValueSwitchSpec extends AnyFlatSpec with Matchers {

  behavior of classOf[ValueSwitch[_]].getSimpleName

  it should "use self-service (v2) error codes" in {
    // given
    val tested = new ValueSwitch[StatusRuntimeException](enableSelfServiceErrorCodes = true)

    // when
    val actual =
      tested.choose(
        v1 = fail("This argument should be evaluated lazily!"),
        v2 = aStatusRuntimeException,
      )

    // then
    actual shouldBe aStatusRuntimeException
  }

  it should "use legacy (v1) error codes" in {
    // given
    val tested = new ValueSwitch[StatusRuntimeException](enableSelfServiceErrorCodes = false)

    // when
    val actual =
      tested.choose(
        v1 = aStatusRuntimeException,
        v2 = fail("This argument should be evaluated lazily!"),
      )

    // then
    actual shouldBe aStatusRuntimeException
  }

  private lazy val aStatusRuntimeException = new StatusRuntimeException(Status.INTERNAL)
}
