// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.lf.data.FlatSpecCheckLaws
import com.daml.http.Generators
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import scalaz.scalacheck.ScalazProperties

class LedgerOffsetUtilTest
    extends FlatSpec
    with Matchers
    with FlatSpecCheckLaws
    with GeneratorDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  import LedgerOffsetUtilTest._

  behavior of LedgerOffsetUtil.AbsoluteOffsetOrdering.getClass.getSimpleName

  checkLaws(ScalazProperties.order.laws[LedgerOffset.Value.Absolute])
}

object LedgerOffsetUtilTest {
  import org.scalacheck.Arbitrary

  implicit val arbAbsoluteOffset: Arbitrary[LedgerOffset.Value.Absolute] = Arbitrary(
    Generators.absoluteLedgerOffsetVal)

  implicit val scalazOrder: scalaz.Order[LedgerOffset.Value.Absolute] =
    scalaz.Order.fromScalaOrdering(LedgerOffsetUtil.AbsoluteOffsetOrdering)
}
