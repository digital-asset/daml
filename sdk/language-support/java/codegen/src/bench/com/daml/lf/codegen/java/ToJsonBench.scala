// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.java

import com.daml.ledger.javaapi.data.Unit
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.Throughput)) @OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@Warmup(iterations = 4)
@Measurement(iterations = 10)
class ToJsonBench {

  @Benchmark
  def enummodBox = Samples.enummodBox.toJson

  @Benchmark
  def enummodOptionalColor = Samples.enummodOptionalColor.toJson

  @Benchmark
  def enummodColoredTree = Samples.enummodColoredTree.toJson

  @Benchmark
  def genmapmodBox = Samples.genmapmodBox.toJson
}

object Samples {

  val enummodOptionalColor = new test.enummod.optionalcolor.SomeColor(test.enummod.Color.GREEN)

  val enummodBox = new test.enummod.Box(test.enummod.Color.RED, "party")

  val enummodColoredTree = new test.enummod.coloredtree.Node(
    test.enummod.Color.BLUE,
    new test.enummod.coloredtree.Leaf(Unit.getInstance()),
    new test.enummod.coloredtree.Leaf(Unit.getInstance()),
  )

  private def pair(i: Long, d: BigDecimal) =
    new test.recordmod.Pair(java.lang.Long.valueOf(i), d.bigDecimal)
  private def right(d: BigDecimal): test.variantmod.Either[java.lang.Long, java.math.BigDecimal] =
    new test.variantmod.either.Right(d.bigDecimal)
  private def left(i: Long): test.variantmod.Either[java.lang.Long, java.math.BigDecimal] =
    new test.variantmod.either.Left(java.lang.Long.valueOf(i))

  import scala.jdk.CollectionConverters._
  val genmapmodBox = new test.genmapmod.Box(
    Map(
      pair(1L, BigDecimal("1.0000000000")) -> right(BigDecimal("1.0000000000")),
      pair(2L, BigDecimal("-2.2222222222")) -> left(2L),
      pair(3L, BigDecimal("3.3333333333")) -> right(BigDecimal("3.3333333333")),
    ).asJava,
    "alice",
  )
}
