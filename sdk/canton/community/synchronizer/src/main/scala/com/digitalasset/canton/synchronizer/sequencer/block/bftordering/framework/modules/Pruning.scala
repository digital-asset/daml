// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, Module}

trait Pruning[E <: Env[E]] extends Module[E, Pruning.Message]

object Pruning {
  sealed trait Message extends Product

  case object PerformPruning extends Message
  final case class LatestBlock(block: OutputBlockMetadata) extends Message
  final case class PruningPoint(epoch: EpochNumber) extends Message
  case object PruningComplete extends Message
}
