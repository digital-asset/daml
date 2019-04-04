// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.util.api

import java.time.Clock

import com.digitalasset.api.util.TimestampConversion
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.{Matchers, WordSpec}

class TimestampConversionTest extends WordSpec with Matchers {

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
