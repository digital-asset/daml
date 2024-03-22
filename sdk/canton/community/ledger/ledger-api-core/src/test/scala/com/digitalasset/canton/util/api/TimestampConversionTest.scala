// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.api

import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Clock

class TimestampConversionTest extends AnyWordSpec with Matchers {

  private val instant = Clock.systemUTC().instant()
  private val timestamp = Timestamp(instant.getEpochSecond, instant.getNano)

  "TimestampConversion" should {

    "convert proto Timestamps to Instant" in {

      TimestampConversion.toInstant(timestamp) shouldEqual instant
    }

    "convert Instants to proto Timestamps" in {

      TimestampConversion.fromInstant(instant) shouldEqual timestamp
    }

  }
}
