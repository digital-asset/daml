// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.java

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.Throughput)) @OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@Warmup(iterations = 4)
@Measurement(iterations = 10)
class FromJsonBench {

  @Benchmark
  def enummodBox = test.enummod.Box.fromJson(JsonSamples.enummodBox)

  @Benchmark
  def enummodOptionalColor = test.enummod.OptionalColor.fromJson(JsonSamples.enummodOptionalColor)

  @Benchmark
  def enummodOptionalColor_ValueBeforeTag =
    test.enummod.OptionalColor.fromJson(JsonSamples.enummodOptionalColor_ValueBeforeTag)

  @Benchmark
  def enummodColoredTree = test.enummod.ColoredTree.fromJson(JsonSamples.enummodColoredTree)

  @Benchmark
  def genmapmodBox = test.genmapmod.Box.fromJson(JsonSamples.genmapmodBox)
}

object JsonSamples {

  val enummodOptionalColor = """{"tag": "SomeColor", "value": "Green"}"""

  val enummodOptionalColor_ValueBeforeTag = """{"value": "Green", "tag": "SomeColor"}"""

  val enummodBox = """{"x": "Red", "party":"party"}"""

  val enummodColoredTree =
    """{
       |  "tag": "Node",
       |  "value": {
       |    "color": "Blue",
       |    "left": {"tag": "Leaf", "value": {}},
       |    "right": {"tag": "Leaf", "value": {}}
       |  }
       |}""".stripMargin

  val genmapmodBox =
    """{
       |  "party": "alice",
       |  "x": [
       |    [ [1, "1.0000000000"], {"tag": "Right", "value": "1.0000000000"} ],
       |    [ [2, "-2.2222222222"], {"tag": "Left", "value": 2} ],
       |    [ [3, "3.3333333333"], {"tag": "Right", "value": "3.3333333333" } ]
       |  ]
       |}""".stripMargin
}
