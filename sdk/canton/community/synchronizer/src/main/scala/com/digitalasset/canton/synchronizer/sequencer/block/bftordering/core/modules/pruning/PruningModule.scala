// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.PruningConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Pruning
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  CancellableEvent,
  Env,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.{Failure, Success}

final class PruningModule[E <: Env[E]](
    config: PruningConfig,
    stores: BftOrderingStores[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends Pruning[E] {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var periodicPruningCancellable: Option[CancellableEvent] = None

  override def receiveInternal(
      message: Pruning.Message
  )(implicit context: E#ActorContextT[Pruning.Message], traceContext: TraceContext): Unit =
    message match {
      case Pruning.Start =>
        pipeToSelf(stores.outputStore.getLowerBound()) {
          case Success(Some(lowerBound)) =>
            Pruning.PerformPruning(lowerBound.epochNumber)
          case Success(None) =>
            Pruning.SchedulePruning
          case Failure(exception) =>
            Pruning.FailedDatabaseOperation(
              "Failed to fetch initial pruning lower bound",
              exception,
            )
        }
      case Pruning.KickstartPruning =>
        pipeToSelf(stores.outputStore.getLastConsecutiveBlock) {
          case Success(Some(block)) =>
            Pruning.ComputePruningPoint(block)
          case Success(None) =>
            logger.warn("No latest block information available to start pruning")
            Pruning.SchedulePruning
          case Failure(exception) =>
            Pruning.FailedDatabaseOperation("Failed to fetch latest block", exception)
        }
      case Pruning.ComputePruningPoint(latestBlock) =>
        val pruningTimestamp = latestBlock.blockBftTime.minus(config.retentionPeriod.toJava)
        val minBlockToKeep = BlockNumber(latestBlock.blockNumber - config.minNumberOfBlocksToKeep)

        pipeToSelf(
          context.zipFuture(
            stores.outputStore.getLatestBlockAtOrBefore(pruningTimestamp),
            stores.outputStore.getBlock(minBlockToKeep),
          )
        ) {
          case Success(result) =>
            result match {
              case (Some(block), None) => Pruning.SaveNewLowerBound(block.epochNumber)
              case (None, Some(block)) => Pruning.SaveNewLowerBound(block.epochNumber)
              case (Some(block1), Some(block2)) =>
                val epoch = EpochNumber(Math.min(block1.epochNumber, block2.epochNumber))
                Pruning.SaveNewLowerBound(epoch)
              case (None, None) =>
                // Could happen if there are not enough blocks yet, so there are no blocks before the pruning point.
                logger.info(s"No pruning point found from latest block ${latestBlock.blockNumber}")
                Pruning.SchedulePruning
            }
          case Failure(exception) =>
            Pruning.FailedDatabaseOperation(
              "Failed to fetch pruning point from database",
              exception,
            )
        }
      case Pruning.SaveNewLowerBound(epochNumber) =>
        pipeToSelf(stores.outputStore.saveLowerBound(epochNumber)) {
          case Success(Right(())) => Pruning.PerformPruning(epochNumber)
          case Success(Left(error)) =>
            logger.error(s"Failed to save new pruning lower bound: $error")
            Pruning.SchedulePruning
          case Failure(exception) =>
            Pruning.FailedDatabaseOperation("Failed to persist new pruning lower bound", exception)
        }
      case Pruning.PerformPruning(epochNumber) =>
        val pruneFuture = context.zipFuture(
          stores.outputStore.prune(epochNumber),
          stores.epochStore.prune(epochNumber),
          // TODO(#23297): add availability stores
        )
        pipeToSelf(pruneFuture) {
          case Success((outputStorePrunedRecords, epochStorePrunedRecords)) =>
            logger.info(
              s"Pruning complete. " +
                s"EpochStore: pruned ${epochStorePrunedRecords.epochs} epochs, ${epochStorePrunedRecords.pbftMessagesCompleted} pbft messages. " +
                s"OutputStore: pruned ${outputStorePrunedRecords.epochs} and ${outputStorePrunedRecords.blocks} blocks."
            )
            Pruning.SchedulePruning
          case Failure(exception) =>
            Pruning.FailedDatabaseOperation("Failed to perform pruning", exception)
        }
      case Pruning.FailedDatabaseOperation(msg, exception) =>
        logger.error(msg, exception)
        schedulePruning()
      case Pruning.SchedulePruning =>
        schedulePruning()
    }

  private def schedulePruning()(implicit
      context: E#ActorContextT[Pruning.Message],
      traceContext: TraceContext,
  ): Unit = {
    logger.info(s"Scheduling pruning in ${config.pruningFrequency}")
    periodicPruningCancellable.foreach(_.cancel())
    periodicPruningCancellable = Some(
      context.delayedEvent(config.pruningFrequency, Pruning.KickstartPruning)
    )
  }
}
