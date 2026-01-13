// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.metrics

import com.digitalasset.canton.ledger.api.benchtool.metrics.MetricsSet
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import scala.concurrent.duration.FiniteDuration

class MetricsSetSpec extends AnyFlatSpec with Matchers {

  it should "convert Scala's FiniteDuration to Java's Duration" in {
    MetricsSet.toJavaDuration(FiniteDuration(5, "seconds")) shouldBe Duration.ofSeconds(5)
  }

}
