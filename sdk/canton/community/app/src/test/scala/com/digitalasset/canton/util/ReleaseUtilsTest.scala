// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.NonEmptyList
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.util.ReleaseUtils.{
  previousSupportedStableReleases,
  reducedScopeOfPreviousSupportedStableReleases,
  shard,
}
import com.digitalasset.canton.version.ReleaseVersion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors}

final class ReleaseUtilsTest extends AnyFlatSpec with Matchers with Inspectors with Inside {

  behavior of "ReleaseUtils"

  it should "only report versions from the current release version" in {

    val previousSupportedStableReleasesMajor =
      previousSupportedStableReleases.map(_.releaseVersion.major)
    all(previousSupportedStableReleasesMajor) shouldBe ReleaseVersion.current.major

    val reducedScopeOfPreviousSupportedStableReleasesMajor =
      reducedScopeOfPreviousSupportedStableReleases.map(_.releaseVersion.major)
    all(reducedScopeOfPreviousSupportedStableReleasesMajor) shouldBe ReleaseVersion.current.major

  }

  it should "correctly identify the latest available patch version for every (major, minor) pair" in {

    val allVersionsByMajorMinor =
      previousSupportedStableReleases.groupBy(v => (v.releaseVersion.major, v.releaseVersion.minor))

    val reducedScopedByMajorMinor =
      reducedScopeOfPreviousSupportedStableReleases.groupBy(v =>
        (v.releaseVersion.major, v.releaseVersion.minor)
      )

    withClue("all (major, minor) pairs covered by the reduced scope") {
      allVersionsByMajorMinor.keys should contain theSameElementsAs reducedScopedByMajorMinor.keys
    }

    withClue("exactly one patch version in the reduced scope") {
      all(reducedScopedByMajorMinor.values) should have length 1
    }

    withClue("only the latest version in the reduced scope") {
      forAll(allVersionsByMajorMinor) { case (majorMinor, patches) =>
        inside(patches.sortBy(_.releaseVersion).reverse) { case latest :: rest =>
          inside(reducedScopedByMajorMinor.get(majorMinor)) { case Some(List(reduced)) =>
            reduced shouldBe latest
            all(rest.map(_.releaseVersion)) should be < reduced.releaseVersion
          }
        }
      }
    }

  }

  private val one = PositiveInt.one
  private val two = PositiveInt.two
  private val three = PositiveInt.tryCreate(3)
  private val four = PositiveInt.tryCreate(4)

  behavior of "sharding"

  it should "shard a single item" in {
    shard(NonEmptyList.of(1), one) shouldBe List(List(1))
  }
  it should "pad with empty lists (shards) when fewer items than requested shards are available" in {
    shard(NonEmptyList.of(1), two) shouldBe List(List(1), List())
  }
  it should "put 1 item per shard when the number of items equals the requested shards" in {
    shard(NonEmptyList.of(1, 2, 3), three) shouldBe List(List(1), List(2), List(3))
  }
  it should "spread items to all shards" in {
    shard(NonEmptyList.of(1, 2, 3, 4), two) shouldBe List(List(1, 2), List(3, 4))

    shard(NonEmptyList.of(1, 2, 3, 4), three) shouldBe List(List(1), List(2), List(3, 4))
    /*
    ^^ shard(list = List(1, 2, 3, 4), n = 3) with
        val itemsPerShard = Math.ceil(list.length / n.toDouble).toInt // = 2
        list.grouped(itemsPerShard)
            .toList
            .padTo(n)

     results in: List(List(1, 2), List(3, 4), List())
     */
  }
  it should "spread items evenly to shards" in {
    shard(NonEmptyList.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), four) shouldBe List(
      List(1, 2),
      List(3, 4),
      List(5, 6, 7),
      List(8, 9, 10),
    )
  }

}
