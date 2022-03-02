// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package explore

import com.daml.bazeltools.BazelRunfiles.{rlocation}

import java.io.File

object SpeedTestNfib extends App {

  val base = "Examples"
  val funcName: String = "nfib"
  val dar = s"daml-lf/interpreter/perf/${base}.dar"
  val darFile = new File(rlocation(dar))

  println(s"Loading Daml function for: $base.$funcName");

  val nfib = LoadDarFunction.load(darFile, base, funcName)

  println(s"Running...");

  val n = 28L

  while (true) {
    val start = System.currentTimeMillis()
    val result = nfib(n)
    val end = System.currentTimeMillis()
    val duration_ms = end - start
    val duration_s = duration_ms.toFloat / 1000.0
    val speed_us = result / (1000 * duration_ms).toFloat
    println(s"n = $n, result = $result, duration(s) = $duration_s, speed(nfibs/us) = $speed_us")
  }

}
