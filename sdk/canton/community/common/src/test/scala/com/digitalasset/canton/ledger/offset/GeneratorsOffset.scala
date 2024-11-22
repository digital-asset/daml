// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.offset

import com.digitalasset.canton.data.AbsoluteOffset
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsOffset {

  implicit val offsetArb: Arbitrary[AbsoluteOffset] = Arbitrary(
    for {
      l <- Gen.posNum[Long]
    } yield AbsoluteOffset.tryFromLong(l)
  )
}
