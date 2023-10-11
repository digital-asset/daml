// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class CantonTimestampTest extends AnyWordSpec with BaseTest {

  "assertFromInstant" should {

    "not fail when the instant must lose precision" in {

      val instantWithNanos = Instant.EPOCH.plusNanos(300L)
      val cantonTimestamp = CantonTimestamp.assertFromInstant(instantWithNanos)
      cantonTimestamp shouldEqual CantonTimestamp.Epoch
    }
  }
}
