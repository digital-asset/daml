// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.openjdk.jmh.annotations._

import java.nio.charset.StandardCharsets
import scala.util.Random

@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 5)
class MessageDigestPrototypeBenchmark {

  var value = ""
  val charset = StandardCharsets.UTF_8

  @Setup
  def setup(): Unit = {
    value = Random.nextString(32)
  }

  @Benchmark
  @Threads(1)
  def newDigest(): Array[Byte] = {
    MessageDigestPrototype.Sha256.newDigest.digest(value.getBytes(charset))
  }

  @Benchmark
  @Threads(4)
  def newDigest4(): Array[Byte] = {
    MessageDigestPrototype.Sha256.newDigest.digest(value.getBytes(charset))
  }

  @Benchmark
  @Threads(16)
  def newDigest16(): Array[Byte] = {
    MessageDigestPrototype.Sha256.newDigest.digest(value.getBytes(charset))
  }

  @Benchmark
  @Threads(32)
  def newDigest32(): Array[Byte] = {
    MessageDigestPrototype.Sha256.newDigest.digest(value.getBytes(charset))
  }
}
