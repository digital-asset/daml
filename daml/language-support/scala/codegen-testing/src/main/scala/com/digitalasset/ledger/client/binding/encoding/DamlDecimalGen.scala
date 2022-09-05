// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.encoding

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import com.daml.ledger.client.binding.{Primitive => P}

// Daml Decimal is DECIMAL(38, 10)
object DamlDecimalGen {

  private val scale = 10

  val MaxDamlDecimal = BigDecimal("9" * 28 + "." + "9" * scale).setScale(scale)
  val MinDamlDecimal = -MaxDamlDecimal

  // this gives us: [-21474836480000000000.0000000000; 21474836470000000000.0000000000]
  // [BigDecimal(scala.Int.MinValue, -10).setScale(10), BigDecimal(scala.Int.MaxValue, -10).setScale(10)]
  private val genDamlDecimal: Gen[BigDecimal] =
    for {
      n <- arbitrary[Int]
      s <- Gen.choose(-scale, scale)
    } yield BigDecimal(n.toLong, s).setScale(scale)

  private val genSpecificDamlDecimal: Gen[BigDecimal] =
    Gen.oneOf(
      MaxDamlDecimal,
      MinDamlDecimal,
      BigDecimal(0).setScale(scale),
      BigDecimal(1).setScale(scale),
      BigDecimal(-1).setScale(scale),
    )

  lazy val arbDamlDecimal: Arbitrary[P.Numeric] = Arbitrary(
    Gen.frequency((10, genDamlDecimal), (5, genSpecificDamlDecimal))
  )
}
