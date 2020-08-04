// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

sealed abstract class Envelope(val name: String) extends Product with Serializable {
  def this(names: Vector[String]) {
    this(names.mkString(Envelope.Separator))
  }
}

object Envelope {

  private val Separator = "."

  private val Prefix = Vector("PerformanceEnvelope")

  val All = Latency.All ++ Throughput.All ++ TransactionSize.All

  sealed abstract class Latency(name: String, val latency: Duration)
      extends Envelope(Latency.Prefix :+ name)

  object Latency {

    private val Prefix = Envelope.Prefix :+ "Latency"

    val All =
      Vector(SixtySeconds, ThreeSeconds, OneSecond, HalfSecond)

    case object SixtySeconds extends Latency("60000ms", latency = Duration(60, TimeUnit.SECONDS))
    case object ThreeSeconds extends Latency("3000ms", latency = Duration(3, TimeUnit.SECONDS))
    case object OneSecond extends Latency("1000ms", latency = Duration(1, TimeUnit.SECONDS))
    case object HalfSecond extends Latency("500ms", latency = Duration(500, TimeUnit.MILLISECONDS))
  }

  sealed abstract class Throughput(name: String, val operationsPerSecond: Int)
      extends Envelope(Throughput.Prefix :+ name)

  object Throughput {

    private val Prefix = Envelope.Prefix :+ "Throughput"

    val All =
      Vector(NoThroughput, FivePerSecond, TwentyPerSecond, FiftyPerSecond, FiveHundredPerSecond)

    case object NoThroughput extends Throughput("ZeroOPS", operationsPerSecond = 0)
    case object FivePerSecond extends Throughput("FiveOPS", operationsPerSecond = 5)
    case object TwentyPerSecond extends Throughput("TwentyOPS", operationsPerSecond = 20)
    case object FiftyPerSecond extends Throughput("FiftyOPS", operationsPerSecond = 50)
    case object FiveHundredPerSecond extends Throughput("FiveHundredOPS", operationsPerSecond = 500)
  }

  sealed abstract class TransactionSize(name: String, val kilobytes: Int)
      extends Envelope(TransactionSize.Prefix :+ name)

  object TransactionSize {

    private val Prefix = Envelope.Prefix :+ "TransactionSize"

    val All =
      Vector(OneKilobyte, OneHundredKilobytes, OneMegabyte, FiveMegabytes, TwentyFiveMegabytes)

    case object OneKilobyte extends TransactionSize("1KB", kilobytes = 1)
    case object OneHundredKilobytes extends TransactionSize("100KB", kilobytes = 100)
    case object OneMegabyte extends TransactionSize("1000KB", kilobytes = 1000)
    case object FiveMegabytes extends TransactionSize("5000KB", kilobytes = 5000)
    case object TwentyFiveMegabytes extends TransactionSize("25000KB", kilobytes = 25000)
  }

}
