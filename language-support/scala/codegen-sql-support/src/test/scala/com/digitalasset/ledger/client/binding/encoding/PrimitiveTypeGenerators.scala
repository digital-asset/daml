// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import com.digitalasset.ledger.client.binding.{Primitive => P}
import org.scalacheck.{Arbitrary, Gen}

object PrimitiveTypeGenerators {

  def primitiveDateGen: Gen[P.Date] = GenEncoding.primitive.valueDate

  def primitiveTimestampGen: Gen[P.Timestamp] = GenEncoding.primitive.valueTimestamp

  object ArbitraryInstances {
    lazy val arbPrimitiveDate: Arbitrary[P.Date] = Arbitrary(GenEncoding.primitive.valueDate)
    lazy val arbPrimitiveTime: Arbitrary[P.Timestamp] = Arbitrary(primitiveTimestampGen)
  }
}
