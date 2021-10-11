// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

/** Other test cases are covered by [[OffsetBuilderSpec]] */
class VersionedOffsetBuilderSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  import VersionedOffsetBuilderSpec._

  "VersionedOffsetBuilder" should {
    "construct and extract" in {
      forAll(arbitrary[Byte], genHighest, Gen.posNum[Int], Gen.posNum[Int]) {
        (version, highest, middle, lowest) =>
          val builder = VersionedOffsetBuilder(version)
          val offset = builder.of(highest, middle, lowest)

          builder.version(offset) should be(version)
          builder.highestIndex(offset) should be(highest)
          builder.middleIndex(offset) should be(middle)
          builder.lowestIndex(offset) should be(lowest)
          builder.split(offset) should be((highest, middle, lowest))
      }
    }

    "fail on a highest that is out of range" in {
      forAll(arbitrary[Byte], genOutOfRangeHighest, Gen.posNum[Int], Gen.posNum[Int]) {
        (version, highest, middle, lowest) =>
          the[IllegalArgumentException] thrownBy VersionedOffsetBuilder(version).of(
            highest,
            middle,
            lowest,
          ) should have message s"requirement failed: highest ($highest) is out of range [0, ${VersionedOffsetBuilder.MaxHighest}]"
      }
    }

    "fail on a negative middle index" in {
      forAll(arbitrary[Byte], genHighest, Gen.negNum[Int], Gen.posNum[Int]) {
        (version, highest, middle, lowest) =>
          the[IllegalArgumentException] thrownBy VersionedOffsetBuilder(version).of(
            highest,
            middle,
            lowest,
          ) should have message s"requirement failed: middle ($middle) is lower than 0"
      }
    }

    "fail on a negative lowest index" in {
      forAll(arbitrary[Byte], genHighest, Gen.posNum[Int], Gen.negNum[Int]) {
        (version, highest, middle, lowest) =>
          the[IllegalArgumentException] thrownBy VersionedOffsetBuilder(version).of(
            highest,
            middle,
            lowest,
          ) should have message s"requirement failed: lowest ($lowest) is lower than 0"
      }
    }

    "fail on a wrong version" in {
      forAll(genHighest, Gen.posNum[Int], Gen.posNum[Int], genDifferentVersions) {
        (highest, middle, lowest, versions) =>
          val offset = VersionedOffsetBuilder(versions._1).of(highest, middle, lowest)
          val offsetBuilder = VersionedOffsetBuilder(versions._2)
          val testedMethods =
            List(offsetBuilder.version(_), offsetBuilder.highestIndex(_), offsetBuilder.split(_))

          testedMethods.foreach { method =>
            the[IllegalArgumentException] thrownBy method(
              offset
            ) should have message s"requirement failed: wrong version ${versions._1}, should be ${versions._2}"
          }
      }
    }
  }
}

object VersionedOffsetBuilderSpec {
  private val genHighest = Gen.chooseNum(0L, VersionedOffsetBuilder.MaxHighest)

  private val genOutOfRangeHighest =
    Gen.oneOf(Gen.negNum[Long], Gen.chooseNum(VersionedOffsetBuilder.MaxHighest + 1, Long.MaxValue))

  private val genDifferentVersions = for {
    version1 <- arbitrary[Byte]
    version2 <- arbitrary[Byte].suchThat(_ != version1)
  } yield (version1, version2)
}
