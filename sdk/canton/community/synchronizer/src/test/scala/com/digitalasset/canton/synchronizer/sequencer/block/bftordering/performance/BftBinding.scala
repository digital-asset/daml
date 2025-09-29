// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance

import java.io.Closeable
import java.util.concurrent.CompletionStage

trait BftBindingFactory {
  type T <: BftBinding

  def create(config: BftBenchmarkConfig): T
}

/** All the operations must be idempotent.
  */
trait BftBinding extends Closeable {

  import BftBinding.*

  def write(
      node: BftBenchmarkConfig.WriteNode[?],
      txId: String,
  ): CompletionStage[Unit]

  def subscribeOnce(node: BftBenchmarkConfig.ReadNode[?], txConsumer: TxConsumer): Unit
}

object BftBinding {

  type TxConsumer = CompletionStage[String] => Unit
}
