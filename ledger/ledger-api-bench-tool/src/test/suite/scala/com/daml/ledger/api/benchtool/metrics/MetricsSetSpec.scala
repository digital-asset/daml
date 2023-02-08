// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import java.time.Duration

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.FiniteDuration

class MetricsSetSpec extends AnyFlatSpec with Matchers {

  it should "convert Scala's FiniteDuration to Java's Duration" in {
    MetricsSet.toJavaDuration(FiniteDuration(5, "seconds")) shouldBe Duration.ofSeconds(5)
  }

}
