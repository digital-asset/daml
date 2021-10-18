// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen

object OffsetGen {
  val genVersion: Gen[Byte] = arbitrary[Byte]

  val genDifferentVersions: Gen[(Byte, Byte)] = for {
    version1 <- genVersion
    version2 <- genVersion.suchThat(_ != version1)
  } yield (version1, version2)

  val genHighest: Gen[Long] = Gen.chooseNum(0L, VersionedOffset.MaxHighest)

  val genOutOfRangeHighest: Gen[Long] =
    Gen.oneOf(Gen.negNum[Long], Gen.chooseNum(VersionedOffset.MaxHighest + 1, Long.MaxValue))

  val genMiddle: Gen[Int] = Gen.chooseNum(0, Int.MaxValue, 0, 1, Int.MaxValue)

  val genOutOfRangeMiddle: Gen[Int] = Gen.negNum[Int]

  val genLowest: Gen[Int] = Gen.chooseNum(0, Int.MaxValue, 0, 1, Int.MaxValue)

  val genOutOfRangeLowest: Gen[Int] = Gen.negNum[Int]
}
