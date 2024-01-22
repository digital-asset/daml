// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.scalatest.FlatSpecCheckLaws
import com.digitalasset.canton.http.Generators
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scalaz.scalacheck.ScalazProperties

class ParticipantOffsetUtilTest
    extends AnyFlatSpec
    with Matchers
    with FlatSpecCheckLaws
    with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  import ParticipantOffsetUtilTest.*

  behavior of ParticipantOffsetUtil.AbsoluteOffsetOrdering.getClass.getSimpleName

  checkLaws(ScalazProperties.order.laws[ParticipantOffset.Value.Absolute])
}

object ParticipantOffsetUtilTest {
  import org.scalacheck.Arbitrary

  implicit val arbAbsoluteOffset: Arbitrary[ParticipantOffset.Value.Absolute] = Arbitrary(
    Generators.absoluteParticipantOffsetVal
  )

  implicit val scalazOrder: scalaz.Order[ParticipantOffset.Value.Absolute] =
    scalaz.Order.fromScalaOrdering(ParticipantOffsetUtil.AbsoluteOffsetOrdering)
}
