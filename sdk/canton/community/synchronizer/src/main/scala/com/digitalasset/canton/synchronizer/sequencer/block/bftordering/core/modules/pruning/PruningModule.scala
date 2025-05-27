// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.PruningConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
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
)(implicit metricsContext: MetricsContext)
    extends Pruning[E] {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var periodicPruningCancellable: Option[CancellableEvent] = None

  override def receiveInternal(
      message: Pruning.Message
  )(implicit context: E#ActorContextT[Pruning.Message], traceContext: TraceContext): Unit =
    message match {
      case Pruning.Start =>
        if (!config.enabled)
          logger.info("Pruning module won't start because pruning is disabled by config")
        else
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
        logger.info(s"Kick-starting new pruning operation")

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
        logger.debug(s"Computing new pruning point from latest block $latestBlock")

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
              case (Some(block1), Some(block2)) =>
                val epoch = EpochNumber(Math.min(block1.epochNumber, block2.epochNumber))
                Pruning.SaveNewLowerBound(epoch)
              case (Some(_), None) =>
                // Pruning cannot be performed in this case, otherwise we would end up with
                // fewer blocks than the minimum number of blocks to keep.
                logger.debug(
                  s"Pruning can't be performed because there are not enough blocks to keep the minimum amount of blocks ${config.minNumberOfBlocksToKeep}"
                )
                Pruning.SchedulePruning
              case (None, Some(_)) =>
                // Pruning cannot be performed in this case, otherwise we would end up pruning blocks inside the
                // retention period allows.
                logger.debug(
                  s"Pruning can't be performed because there are no blocks to be pruned outside the retention period of ${config.retentionPeriod}"
                )
                Pruning.SchedulePruning
              case (None, None) =>
                // Could happen if there are not enough blocks yet, so there are no blocks before the pruning point.
                logger.debug(s"No pruning point found from latest block ${latestBlock.blockNumber}")
                Pruning.SchedulePruning
            }
          case Failure(exception) =>
            Pruning.FailedDatabaseOperation(
              "Failed to fetch pruning point from database",
              exception,
            )
        }
      case Pruning.SaveNewLowerBound(epochNumber) =>
        logger.debug(s"Saving new lower bound $epochNumber")
        pipeToSelf(stores.outputStore.saveLowerBound(epochNumber)) {
          case Success(Right(())) => Pruning.PerformPruning(epochNumber)
          case Success(Left(error)) =>
            logger.error(s"Failed to save new pruning lower bound: $error")
            Pruning.SchedulePruning
          case Failure(exception) =>
            Pruning.FailedDatabaseOperation("Failed to persist new pruning lower bound", exception)
        }
      case Pruning.PerformPruning(epochNumber) =>
        logger.info(s"Pruning at epoch $epochNumber starting")
        val pruneFuture = context.zipFuture(
          stores.outputStore.prune(epochNumber),
          stores.epochStore.prune(epochNumber),
          stores.availabilityStore.prune(
            EpochNumber(epochNumber - OrderingRequestBatch.BatchValidityDurationEpochs + 1L)
          ),
        )
        pipeToSelf(pruneFuture) {
          case Success(
                (outputStorePrunedRecords, epochStorePrunedRecords, availabilityStorePrunedRecords)
              ) =>
            logger.info(
              s"""|Pruning at epoch $epochNumber complete.
                  |EpochStore: pruned ${epochStorePrunedRecords.epochs} epochs, ${epochStorePrunedRecords.pbftMessagesCompleted} pbft messages.
                  |OutputStore: pruned ${outputStorePrunedRecords.epochs} and ${outputStorePrunedRecords.blocks} blocks.
                  |AvailabilityStore: pruned ${availabilityStorePrunedRecords.batches} batches.""".stripMargin
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
