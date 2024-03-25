// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.openjdk.jmh.annotations._

import java.nio.charset.StandardCharsets
import javax.crypto.spec.SecretKeySpec
import scala.util.Random

@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 5)
class MacPrototypeBenchmark {

  var key = ""
  var value = ""
  val charset = StandardCharsets.UTF_8

  @Setup
  def setup(): Unit = {
    key = Random.nextString(32)
    value = Random.nextString(32)
  }

  private def encode(): Array[Byte] = {
    val prototype = MacPrototype.HmacSha256
    val mac = prototype.newMac
    mac.init(new SecretKeySpec(key.getBytes(charset), prototype.algorithm))
    mac.doFinal(value.getBytes(charset))
  }

  @Benchmark
  @Threads(1)
  def encodeHmacSHA_256_1(): Array[Byte] = {
    encode()
  }

  @Benchmark
  @Threads(4)
  def encodeHmacSHA_256_4(): Array[Byte] = {
    encode()
  }

  @Benchmark
  @Threads(16)
  def encodeHmacSHA_256_16(): Array[Byte] = {
    encode()
  }
}
