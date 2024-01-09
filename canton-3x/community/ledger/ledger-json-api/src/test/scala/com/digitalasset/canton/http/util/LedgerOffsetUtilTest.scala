// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.scalatest.FlatSpecCheckLaws
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.canton.http.Generators
import com.digitalasset.canton.http.util.LedgerOffsetUtil
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.scalacheck.ScalazProperties

class LedgerOffsetUtilTest
    extends AnyFlatSpec
    with Matchers
    with FlatSpecCheckLaws
    with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  import LedgerOffsetUtilTest._

  behavior of LedgerOffsetUtil.AbsoluteOffsetOrdering.getClass.getSimpleName

  checkLaws(ScalazProperties.order.laws[LedgerOffset.Value.Absolute])
}

object LedgerOffsetUtilTest {
  import org.scalacheck.Arbitrary

  implicit val arbAbsoluteOffset: Arbitrary[LedgerOffset.Value.Absolute] = Arbitrary(
    Generators.absoluteLedgerOffsetVal
  )

  implicit val scalazOrder: scalaz.Order[LedgerOffset.Value.Absolute] =
    scalaz.Order.fromScalaOrdering(LedgerOffsetUtil.AbsoluteOffsetOrdering)
}
