// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.*
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsConfig {
  // Refined Int
  implicit val nonNegativeIntArb: Arbitrary[NonNegativeInt] = Arbitrary(
    Gen.choose(0, Int.MaxValue).map(NonNegativeInt.tryCreate)
  )
  implicit val positiveIntArb: Arbitrary[PositiveInt] = Arbitrary(
    Gen.choose(1, Int.MaxValue).map(PositiveInt.tryCreate)
  )

  // Refined Long
  implicit val nonNegativeLongArb: Arbitrary[NonNegativeLong] = Arbitrary(
    Gen.choose(0, Long.MaxValue).map(NonNegativeLong.tryCreate)
  )
  implicit val positiveLongArb: Arbitrary[PositiveLong] = Arbitrary(
    Gen.choose(1, Long.MaxValue).map(PositiveLong.tryCreate)
  )
}
