// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.PruningConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Pruning
import com.digitalasset.canton.tracing.TraceContext

import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.{Failure, Success}

final class PruningModule[E <: Env[E]](
    config: PruningConfig,
    stores: BftOrderingStores[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends Pruning[E] {

  override def receiveInternal(
      message: Pruning.Message
  )(implicit context: E#ActorContextT[Pruning.Message], traceContext: TraceContext): Unit =
    message match {
      // TODO(#23297): schedule pruning
      case Pruning.PerformPruning =>
        context.pipeToSelf(stores.outputStore.getLastConsecutiveBlock) {
          case Success(Some(block)) =>
            Some(Pruning.LatestBlock(block))
          case Success(None) =>
            logger.warn("No latest block information available to start pruning")
            Some(Pruning.PruningComplete)
          case Failure(exception) =>
            logger.error("Failed to fetch latest block", exception)
            None
        }
      case Pruning.LatestBlock(latestBlock) =>
        val pruningTimestamp = latestBlock.blockBftTime.minus(config.retentionPeriod.toJava)
        val minBlockToKeep = BlockNumber(latestBlock.blockNumber - config.minNumberOfBlocksToKeep)

        context.pipeToSelf(
          context.zipFuture(
            stores.outputStore.getLatestBlockAtOrBefore(pruningTimestamp),
            stores.outputStore.getBlock(minBlockToKeep),
          )
        ) {
          case Success(result) =>
            Some(result match {
              case (Some(block), None) => Pruning.PruningPoint(block.epochNumber)
              case (None, Some(block)) => Pruning.PruningPoint(block.epochNumber)
              case (Some(block1), Some(block2)) =>
                val epoch = EpochNumber(Math.min(block1.epochNumber, block2.epochNumber))
                Pruning.PruningPoint(epoch)
              case (None, None) =>
                // Could happen if there are not enough blocks yet, so there are no blocks before the pruning point.
                logger.info(s"No pruning point found from latest block ${latestBlock.blockNumber}")
                Pruning.PruningComplete
            })
          case Failure(exception) =>
            logger.error("Failed to fetch pruning point from database", exception)
            None
        }
      case Pruning.PruningPoint(epochNumber) =>
        // TODO(#23297): add other stores to be pruned
        context.pipeToSelf(stores.epochStore.prune(EpochNumber(epochNumber - 1))) {
          case Success(prunedRecords) =>
            logger.info(
              s"Pruned ${prunedRecords.epochs} epochs, ${prunedRecords.pbftMessagesCompleted} pbft messages"
            )
            Some(Pruning.PruningComplete)
          case Failure(exception) =>
            logger.error("Failed to perform pruning", exception)
            None
        }
      case Pruning.PruningComplete =>
      // TODO(#23297): re-schedule pruning
    }

}
