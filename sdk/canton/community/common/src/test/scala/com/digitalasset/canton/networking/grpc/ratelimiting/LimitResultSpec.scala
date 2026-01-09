// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import com.digitalasset.base.error.{ContextualizedDamlError, ErrorCategory, ErrorClass, ErrorCode}
import com.digitalasset.canton.logging.NoLogging
import com.digitalasset.canton.networking.grpc.ratelimiting.LimitResult.{OverLimit, UnderLimit}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LimitResultSpec extends AnyFlatSpec with Matchers {

  behavior of "LimitResult"

  def underCheck(): LimitResult = UnderLimit
  def overCheck(method: String): LimitResult = OverLimit(
    LimitResultSpec.TestError.Err(method)
  )

  it should "compose under limit" in {
    val actual: LimitResult = for {
      _ <- underCheck()
      _ <- underCheck()
    } yield ()
    actual shouldBe UnderLimit
  }

  it should "compose over limit" in {
    val expected = overCheck("First issue")
    val actual: LimitResult = for {
      _ <- underCheck()
      _ <- expected
      _ <- underCheck()
      _ <- overCheck("Other failure")
    } yield ()
    actual shouldBe expected
  }

}

object LimitResultSpec {
  private object TestError
      extends ErrorCode(
        id = "IGNORE_ME",
        ErrorCategory.ContentionOnSharedResources,
      )(ErrorClass.root()) {
    final case class Err(name: String)
        extends ContextualizedDamlError(
          cause = ""
        )(TestError.this, NoLogging)
  }
}
