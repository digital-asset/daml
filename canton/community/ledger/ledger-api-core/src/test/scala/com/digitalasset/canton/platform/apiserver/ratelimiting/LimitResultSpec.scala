// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.ratelimiting

import com.daml.error.NoLogging
import com.digitalasset.canton.ledger.error.LedgerApiErrors.MaximumNumberOfStreams
import com.digitalasset.canton.platform.apiserver.ratelimiting.LimitResult.{OverLimit, UnderLimit}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LimitResultSpec extends AnyFlatSpec with Matchers {

  behavior of "LimitResult"

  def underCheck(): LimitResult = UnderLimit
  def overCheck(method: String): LimitResult = OverLimit(
    MaximumNumberOfStreams.Rejection(0, 0, "", method)(NoLogging)
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
