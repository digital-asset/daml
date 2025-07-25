// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, Module}

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

trait Pruning[E <: Env[E]] extends Module[E, Pruning.Message]

object Pruning {
  sealed trait Message extends Product

  case object Start extends Message
  final case class PruningStatusRequest(promise: Promise[OutputMetadataStore.LowerBound])
      extends Message

  final case class KickstartPruning(
      retention: FiniteDuration,
      minBlocksToKeep: Int,
      promise: Option[Promise[String]],
  ) extends Message

  final case class ComputePruningPoint(
      block: OutputBlockMetadata,
      retention: FiniteDuration,
      minBlocksToKeep: Int,
  ) extends Message
  final case class SaveNewLowerBound(epoch: EpochNumber) extends Message
  final case class PerformPruning(epoch: EpochNumber) extends Message
  final case class FailedDatabaseOperation(msg: String, exception: Throwable) extends Message
  case object SchedulePruning extends Message
}
