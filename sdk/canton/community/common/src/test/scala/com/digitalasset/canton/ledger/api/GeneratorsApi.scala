// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.digitalasset.canton.ledger.api.DeduplicationPeriod.DeduplicationDuration
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import java.time.Duration

object GeneratorsApi {
  import com.digitalasset.canton.ledger.offset.GeneratorsOffset.*

  implicit val deduplicationDurationArb: Arbitrary[DeduplicationDuration] = Arbitrary(
    Gen.posNum[Long].map(Duration.ofMillis).map(DeduplicationDuration)
  )
  implicit val deduplicationPeriodArb: Arbitrary[DeduplicationPeriod] = genArbitrary
}
