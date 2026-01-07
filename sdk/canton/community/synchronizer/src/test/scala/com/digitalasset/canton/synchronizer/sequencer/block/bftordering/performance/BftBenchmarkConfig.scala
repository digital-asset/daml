// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance

import scala.concurrent.duration.Duration

final case class BftBenchmarkConfig(
    transactionBytes: Int,
    runDuration: Duration,
    perNodeWritePeriod: Duration,
    nodes: Seq[BftBenchmarkConfig.Node],
    reportingInterval: Option[Duration] = None,
)

object BftBenchmarkConfig {

  sealed trait Node extends Product {
    val host: String
  }

  sealed trait ReadNode[PortType] extends Node {
    val readPort: PortType
  }

  sealed trait WriteNode[PortType] extends Node {
    val writePort: PortType
  }

  final case class InProcessWriteOnlyNode(override val host: String, override val writePort: String)
      extends WriteNode[String]

  final case class InProcessReadWriteNode(
      override val host: String,
      override val writePort: String,
      override val readPort: String,
  ) extends WriteNode[String]
      with ReadNode[String]

  final case class NetworkedWriteOnlyNode(override val host: String, override val writePort: Int)
      extends WriteNode[Int]

  final case class NetworkedReadWriteNode(
      override val host: String,
      override val writePort: Int,
      override val readPort: Int,
  ) extends WriteNode[Int]
      with ReadNode[Int]
}
