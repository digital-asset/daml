// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, Module}

trait Pruning[E <: Env[E]] extends Module[E, Pruning.Message]

object Pruning {
  sealed trait Message extends Product

  case object Start extends Message
  case object KickstartPruning extends Message
  final case class ComputePruningPoint(block: OutputBlockMetadata) extends Message
  final case class SaveNewLowerBound(epoch: EpochNumber) extends Message
  final case class PerformPruning(epoch: EpochNumber) extends Message
  final case class FailedDatabaseOperation(msg: String, exception: Throwable) extends Message
  case object SchedulePruning extends Message
}
