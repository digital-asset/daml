// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import org.openjdk.jmh.annotations.*

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.Random

// The benchmarks are created with the use of the JMH harness and one can run them from the
// sbt console by running:
//    sbt:canton> participant-integration-api-bench/Jmh/run
@SuppressWarnings(
  Array("org.wartremover.warts.Var", "org.wartremover.warts.Null")
)
@State(Scope.Benchmark)
abstract class BenchmarkState {
  @Param(Array("10000", "100000", "1000000", "10000000"))
  var stringCount: Int = _

  @Param(Array("10", "100"))
  var stringLength: Int = _

  protected val perfTestTimeout: FiniteDuration = 5.minutes

  protected var entries: Array[(Int, String)] = _
  protected var interning: StringInterningView = _
  protected var interningEnd: Int = _

  protected def extraStringCount = 0

  @Setup(Level.Trial)
  def setupEntries(): Unit =
    entries = BenchmarkState.createEntries(stringCount + extraStringCount, stringLength)
}

object BenchmarkState {

  protected val perfTestTimeout: FiniteDuration = 5.minutes

  private[this] def randomString(length: Int): String = Random.alphanumeric.take(length).mkString

  def createEntries(stringCount: Int, stringLength: Int): Array[(Int, String)] = {
    Console.print(
      s"Creating an array with $stringCount entries with string length $stringLength..."
    )

    val entries = new Array[(Int, String)](stringCount)
    (0 until stringCount).foreach(i => entries(i) = (i + 1) -> randomString(stringLength))
    Console.println(s" done.")

    Console.println(s"First few entries: ${entries(0)}, ${entries(1)}, ${entries(2)}, ...")
    entries
  }

  def loadStringInterningEntries(
      entries: Array[(Int, String)]
  ): LoadStringInterningEntries = { (fromExclusive, toInclusive) =>
    // Note: for slice(), the begin is inclusive and the end is exclusive (opposite of the enclosing call)
    Future.successful(entries.view.slice(fromExclusive + 1, toInclusive + 1))
  }
}
