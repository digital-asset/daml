// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import org.scalacheck.{Arbitrary, Gen}

import java.time.Duration

object GeneratorsDataTime {
  private val tenYears: Duration = Duration.ofDays(365 * 10)

  implicit val cantonTimestampArb: Arbitrary[CantonTimestamp] = Arbitrary(
    Gen.choose(0, tenYears.getSeconds * 1000 * 1000).map(CantonTimestamp.ofEpochMicro)
  )
  implicit val cantonTimestampSecondArb: Arbitrary[CantonTimestampSecond] = Arbitrary(
    Gen.choose(0, tenYears.getSeconds).map(CantonTimestampSecond.ofEpochSecond)
  )
}
