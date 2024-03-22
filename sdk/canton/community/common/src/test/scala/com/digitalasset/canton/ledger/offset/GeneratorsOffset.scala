// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.offset

import com.daml.lf.data.Ref
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsOffset {

  implicit val offsetArb: Arbitrary[Offset] = Arbitrary(
    for {
      len <- Gen.oneOf(4, 8, 12, 16)
      str <- Gen.stringOfN(len, Gen.hexChar)
    } yield com.digitalasset.canton.ledger.offset.Offset
      .fromHexString(Ref.HexString.assertFromString(str.toLowerCase))
  )
}
