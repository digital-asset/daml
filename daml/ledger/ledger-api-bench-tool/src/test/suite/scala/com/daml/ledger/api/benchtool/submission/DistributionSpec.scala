// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class DistributionSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  "Distribution" should {
    val MaxValue = 1000000
    val smallInt = Gen.choose(1, MaxValue)
    val zeroToOneDouble: Gen[Double] =
      Gen.choose(0, Int.MaxValue - 1).map(_.toDouble / Int.MaxValue)
    val listOfWeights: Gen[List[Int]] = Gen.choose(1, 50).flatMap(Gen.listOfN(_, smallInt))

    "handle single-element list" in {
      val cases: Gen[(Int, Double)] = for {
        weight <- smallInt
        double <- zeroToOneDouble
      } yield (weight, double)

      forAll(cases) { case (weight, d) =>
        val sentinel = new Object()
        val distribution = new Distribution[Object](List(weight), IndexedSeq(sentinel))
        distribution.choose(d) shouldBe sentinel
      }
    }

    "handle multi-element list" in {
      val cases = for {
        double <- zeroToOneDouble
        weights <- listOfWeights
      } yield (weights, double)

      forAll(cases) { case (weights, d) =>
        val distribution = new Distribution[Int](weights, items = weights.toIndexedSeq)
        val index = distribution.index(d)

        val totalWeight = weights.map(_.toLong).sum
        weights.take(index).map(_.toDouble / totalWeight).sum should be <= d
        weights.take(index + 1).map(_.toDouble / totalWeight).sum should be > d
      }
    }
  }

}
