// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package data

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.util.Random

@State(Scope.Benchmark)
class TreeMapBench {

  type TreeMap[K, V] = collection.immutable.TreeMap[K, V]

  val AnyMapOrdering: Ordering[TreeMap[Any, Any]] = new Ordering[TreeMap[Any, Any]] {
    override def compare(x: TreeMap[Any, Any], y: TreeMap[Any, Any]): Int = {
      assert(x.ordering == y.ordering)

      @annotation.tailrec
      def loop(keys1: List[Any], keys2: List[Any]): Int =
        (keys1, keys2) match {
          case (k1 :: ks1, k2 :: ks2) =>
            val c = x.ordering.compare(k1, k2)
            // if the first elements are equal, compare the rest of the lists recursively
            if (c == 0) loop(ks1, ks2)
            // otherwise, return the comparison result of the first elements
            else c
          case (Nil, Nil) => 0
          case (Nil, _) => -1
          case (_, Nil) => 1
        }

      loop(x.keys.toList, y.keys.toList)
    }
  }

  implicit def mapOrdering[K, V]: Ordering[TreeMap[K, V]] =
    AnyMapOrdering.asInstanceOf[Ordering[TreeMap[K, V]]]

  val seed = getClass.getName.iterator.toSeq.hashCode()

  val random = new Random(seed)

  def examples0(n: Int): Iterator[(Long, Long)] =
    Iterator.fill(n)(random.nextLong(n * n.toLong) -> random.nextLong(n.toLong))

  def example1(n: Int): Iterator[(TreeMap[Long, Long], Long)] =
    Iterator
      .iterate(immutable.TreeMap.empty[Long, Long], n)(
        _.updated(random.nextLong(n * n.toLong), random.nextLong(n.toLong))
      )
      .map(_ -> random.nextLong(n.toLong))

  @Param(Array("1", "2", "4", "8", "16", "32", "64", "128", "1024", "2048", "4096"))
  var n: Int = _

  var smallEntries: Array[(Long, Long)] = _
  var bigEntries: Array[(TreeMap[Long, Long], Long)] = _

  @Setup(Level.Trial)
  def init(): Unit = {
    smallEntries = examples0(n).toArray.sorted
    assert(TreeMap.fromOrderedEntries(smallEntries) == immutable.TreeMap.from(smallEntries))
    bigEntries = example1(n).toArray.sorted
    assert(TreeMap.fromOrderedEntries(bigEntries) == immutable.TreeMap.from(bigEntries))
  }

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def smallBenchN(blackhole: Blackhole): Unit =
    blackhole.consume(TreeMap.fromOrderedEntries(smallEntries))

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def smallBenchNLogN(blackhole: Blackhole): Unit =
    blackhole.consume(immutable.TreeMap.from(smallEntries))

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def bigBenchN(blackhole: Blackhole): Unit =
    blackhole.consume(TreeMap.fromOrderedEntries(bigEntries))

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def bigBenchNLogN(blackhole: Blackhole): Unit =
    blackhole.consume(immutable.TreeMap.from(bigEntries))

}
