// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MetricsContextSpec extends AnyWordSpec with Matchers {

  "merging metrics contexts" should {

    "merge all labels" in {
      MetricsContext(
        Map("key" -> "value")
      ).merge(
        MetricsContext(
          Map("key2" -> "value")
        )
      ) should equal(
        MetricsContext(
          Map(
            "key" -> "value",
            "key2" -> "value",
          )
        )
      )
    }

    "handle duplicate labels" in {
      MetricsContext(
        Map("key" -> "value1")
      ).merge(
        MetricsContext(
          Map("key" -> "value2")
        )
      ) should equal(
        MetricsContext(
          Map(
            "key" -> "value2"
          )
        )
      )
    }
  }

}
