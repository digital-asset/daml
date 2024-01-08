// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import org.scalacheck.Arbitrary

import java.time.Duration

object GeneratorsTime {
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import org.scalatest.EitherValues.*

  implicit val nonNegativeSecondsArb: Arbitrary[NonNegativeSeconds] = Arbitrary(
    nonNegativeLongArb.arbitrary.map(i => NonNegativeSeconds.tryOfSeconds(i.unwrap))
  )

  implicit val nonNegativeFiniteDurationArb: Arbitrary[NonNegativeFiniteDuration] = Arbitrary(
    nonNegativeLongArb.arbitrary.map(i =>
      NonNegativeFiniteDuration.create(Duration.ofNanos(i.unwrap)).value
    )
  )

  implicit val positiveFiniteDurationArb: Arbitrary[PositiveFiniteDuration] = Arbitrary(
    nonNegativeLongArb.arbitrary.map(i =>
      PositiveFiniteDuration.create(Duration.ofNanos(i.unwrap)).value
    )
  )

  implicit val positiveSecondsArb: Arbitrary[PositiveSeconds] = Arbitrary(
    positiveLongArb.arbitrary.map(i => PositiveSeconds.tryOfSeconds(i.unwrap))
  )
}
