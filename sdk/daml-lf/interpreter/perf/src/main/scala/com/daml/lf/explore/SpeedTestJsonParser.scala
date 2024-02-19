// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package explore

import com.daml.bazeltools.BazelRunfiles.{rlocation}

import java.io.File

object SpeedTestJsonParser extends App {

  val base = "JsonParser"
  val funcName: String = "pipeline"
  val dar = s"daml-lf/interpreter/perf/${base}.dar"
  val darFile = new File(rlocation(dar))

  println(s"Loading Daml function for: $base.$funcName");

  val pipeline = LoadDarFunction.load(darFile, base, funcName)

  println(s"Running...");

  val n = 10L

  while (true) {
    val start = System.currentTimeMillis()
    val size = pipeline(n)
    val end = System.currentTimeMillis()
    val size_k = size.toFloat / 1e3
    val duration_ms = end - start
    val duration_s = duration_ms.toFloat / 1000.0
    val speed_k_per_s = size_k / duration_s
    println(s"n = $n, size(k) = $size_k, duration(s) = $duration_s, speed(k/s) = $speed_k_per_s")
  }

}
