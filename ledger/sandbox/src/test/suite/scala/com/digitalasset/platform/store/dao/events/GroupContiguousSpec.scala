// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao.events

import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.PropertyChecks
import org.scalatest.{AsyncFlatSpec, Matchers}

final class GroupContiguousSpec
    extends AsyncFlatSpec
    with Matchers
    with PropertyChecks
    with ScalaFutures
    with AkkaBeforeAndAfterAll {

  behavior of "groupContiguous"

  it should "be equivalent to grouping on inputs with an ordered key" in forAll {
    pairs: List[(Int, String)] =>
      val sortedPairs = pairs.sortBy(_._1)
      val grouped = groupContiguous(Source(sortedPairs))(by = _._1)
      whenReady(grouped.runWith(Sink.seq[Vector[(Int, String)]])) {
        _ should contain theSameElementsAs pairs.groupBy(_._1).values
      }
  }

  it should "be equivalent to grouping on inputs with a contiguous key" in {
    val pairsWithContiguousKeys = List(1 -> "baz", 0 -> "foo", 0 -> "bar", 0 -> "quux")
    val grouped = groupContiguous(Source(pairsWithContiguousKeys))(by = _._1)
    whenReady(grouped.runWith(Sink.seq[Vector[(Int, String)]])) {
      _ should contain theSameElementsAs pairsWithContiguousKeys.groupBy(_._1).values
    }
  }

  it should "behave as expected when grouping inputs without a contiguous key" in {
    val pairs = List(0 -> "foo", 0 -> "bar", 1 -> "baz", 0 -> "quux")
    val grouped = groupContiguous(Source(pairs))(by = _._1)
    whenReady(grouped.runWith(Sink.seq[Vector[(Int, String)]])) {
      _ should contain theSameElementsAs Vector(
        Vector(0 -> "foo", 0 -> "bar"),
        Vector(1 -> "baz"),
        Vector(0 -> "quux"),
      )
    }
  }

}
