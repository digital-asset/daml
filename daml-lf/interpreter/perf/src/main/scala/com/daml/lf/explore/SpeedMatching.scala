// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package explore

import com.daml.bazeltools.BazelRunfiles.{rlocation}

import java.io.File

object SpeedMatching extends App {

  val base = "Examples"
  val funcName: String = "patTest20"
  val dar = s"daml-lf/interpreter/perf/${base}.dar"
  val darFile = new File(rlocation(dar))

  println(s"Loading DAML function for: $base.$funcName");

  val patTest = LoadDarFunction.load(darFile, base, funcName)

  println(s"Running...");

  val n = 100000L

  while (true) {
    val start = System.currentTimeMillis()
    val result = patTest(n)
    val end = System.currentTimeMillis()
    val duration_ms = end - start
    val duration_s = duration_ms.toFloat / 1000.0
    val speed_ms = result / duration_ms.toFloat
    println(s"n = $n, result = $result, duration(s) = $duration_s, speed(matches/ms) = $speed_ms")
  }

}
