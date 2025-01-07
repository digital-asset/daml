// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.offset

import com.digitalasset.canton.data.Offset
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsOffset {

  implicit val offsetArb: Arbitrary[Offset] = Arbitrary(
    for {
      l <- Gen.posNum[Long]
    } yield Offset.tryFromLong(l)
  )
}
