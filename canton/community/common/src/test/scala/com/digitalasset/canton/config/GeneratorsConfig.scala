// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.CantonRequireTypes.{String185, String255, String68}
import com.digitalasset.canton.config.RequireTypes.*
import com.digitalasset.canton.{Generators, config}
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.duration

object GeneratorsConfig {
  import org.scalatest.EitherValues.*

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

  implicit val string68Arb: Arbitrary[String68] = Arbitrary(
    Generators.lengthLimitedStringGen(String68)
  )
  implicit val string185Arb: Arbitrary[String185] = Arbitrary(
    Generators.lengthLimitedStringGen(String185)
  )
  implicit val string255Arb: Arbitrary[String255] = Arbitrary(
    Generators.lengthLimitedStringGen(String255)
  )

  implicit val nonNegativeFiniteDurationArb: Arbitrary[config.NonNegativeFiniteDuration] =
    Arbitrary(
      Arbitrary
        .arbitrary[NonNegativeLong]
        .map(i => scala.concurrent.duration.FiniteDuration(i.unwrap, duration.NANOSECONDS))
        .map(d => config.NonNegativeFiniteDuration.fromDuration(d).value)
    )
}
