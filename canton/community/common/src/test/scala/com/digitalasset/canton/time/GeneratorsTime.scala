// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.TargetDomainId
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import java.time.Duration

object GeneratorsTime {
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.data.GeneratorsData.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
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

  implicit val timeProofArb: Arbitrary[TimeProof] = Arbitrary(for {
    timestamp <- Arbitrary.arbitrary[CantonTimestamp]
    counter <- Gen.long
    targetDomain <- Arbitrary.arbitrary[TargetDomainId]
  } yield TimeProofTestUtil.mkTimeProof(timestamp, counter, targetDomain))
}
