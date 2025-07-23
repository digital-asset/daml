// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.PruningConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
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

import scala.concurrent.Promise
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

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var pruningResultPromise: Option[Promise[String]] = None

  private def completePruningOperation(message: String): Unit = {
    pruningResultPromise.foreach(_.complete(Success(message)))
    pruningResultPromise = None
  }

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
      case Pruning.KickstartPruning(retention, minBlocksToKeep, requestPromise) =>
        def kickStartPruning(): Unit = {
          logger.info(s"Kick-starting new pruning operation")
          pipeToSelf(stores.outputStore.getLastConsecutiveBlock) {
            case Success(Some(block)) =>
              Pruning.ComputePruningPoint(block, retention, minBlocksToKeep)
            case Success(None) =>
              completePruningOperation("No latest block information available to start pruning")
              logger.warn("No latest block information available to start pruning")
              Pruning.SchedulePruning
            case Failure(exception) =>
              Pruning.FailedDatabaseOperation("Failed to fetch latest block", exception)
          }
        }
        (pruningResultPromise, requestPromise) match {
          case (None, _) =>
            pruningResultPromise = requestPromise
            kickStartPruning()
          case (Some(_), None) =>
            // TODO(i26216): better handle case of when scheduled pruning starts when a admin command pruning operation is in-progress
            kickStartPruning()
          case (Some(_), Some(promise)) =>
            // TODO(i26216): better handle case starting an operation when another is taking place
            promise.complete(
              Success("Pruning won't continue because an operation is already taking place")
            )
        }
      case Pruning.ComputePruningPoint(latestBlock, retention, minBlocksToKeep) =>
        logger.debug(s"Computing new pruning point from latest block $latestBlock")

        val pruningTimestamp = latestBlock.blockBftTime.minus(retention.toJava)
        val minBlockToKeep = BlockNumber(latestBlock.blockNumber - minBlocksToKeep)

        pipeToSelf(
          context.zipFuture(
            stores.outputStore.getLatestBlockAtOrBefore(pruningTimestamp),
            stores.outputStore.getBlock(minBlockToKeep),
            orderingStage = Some("pruning-compute-pruning-point"),
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
                val msg =
                  s"Pruning can't be performed because there are not enough blocks to keep the minimum amount of blocks $minBlocksToKeep"
                completePruningOperation(msg)
                logger.debug(msg)
                Pruning.SchedulePruning
              case (None, Some(_)) =>
                // Pruning cannot be performed in this case, otherwise we would end up pruning blocks inside the
                // retention period allows.
                val msg =
                  s"Pruning can't be performed because there are no blocks to be pruned outside the retention period of $retention"
                completePruningOperation(msg)
                logger.debug(msg)
                Pruning.SchedulePruning
              case (None, None) =>
                // Could happen if there are not enough blocks yet, so there are no blocks before the pruning point.
                val msg = s"No pruning point found from latest block ${latestBlock.blockNumber}"
                logger.debug(msg)
                completePruningOperation(msg)
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
            val msg = s"Failed to save new pruning lower bound: $error"
            completePruningOperation(msg)
            logger.error(msg)
            Pruning.SchedulePruning
          case Failure(exception) =>
            Pruning.FailedDatabaseOperation("Failed to persist new pruning lower bound", exception)
        }
      case Pruning.PerformPruning(epochNumber) =>
        logger.info(s"Pruning at epoch $epochNumber starting")
        val pruneFuture =
          context.zipFuture3(
            stores.outputStore.prune(epochNumber),
            stores.epochStore.prune(epochNumber),
            stores.availabilityStore.prune(
              EpochNumber(epochNumber - OrderingRequestBatch.BatchValidityDurationEpochs + 1L)
            ),
            orderingStage = Some("pruning-prune"),
          )
        pipeToSelf(pruneFuture) {
          case Success(
                (outputStorePrunedRecords, epochStorePrunedRecords, availabilityStorePrunedRecords)
              ) =>
            val msg = s"""|Pruning at epoch $epochNumber complete.
                          |EpochStore: pruned ${epochStorePrunedRecords.epochs} epochs, ${epochStorePrunedRecords.pbftMessagesCompleted} pbft messages.
                          |OutputStore: pruned ${outputStorePrunedRecords.epochs} epochs and ${outputStorePrunedRecords.blocks} blocks.
                          |AvailabilityStore: pruned ${availabilityStorePrunedRecords.batches} batches.""".stripMargin
            logger.info(msg)
            completePruningOperation(msg)
            Pruning.SchedulePruning
          case Failure(exception) =>
            Pruning.FailedDatabaseOperation("Failed to perform pruning", exception)
        }
      case Pruning.FailedDatabaseOperation(msg, exception) =>
        completePruningOperation(msg)
        logger.error(msg, exception)
        schedulePruning()
      case Pruning.SchedulePruning =>
        schedulePruning()

      case Pruning.PruningStatusRequest(promise) =>
        pipeToSelfOpt(stores.outputStore.getLowerBound()) { result =>
          (result match {
            case Success(Some(lowerBound)) => promise.complete(Success(lowerBound))
            case Success(None) =>
              val lowerBound = OutputMetadataStore.LowerBound(EpochNumber.First, BlockNumber.First)
              promise.complete(Success(lowerBound))
            case Failure(exception) => promise.failure(exception)
          }).discard
          None
        }
    }

  private def schedulePruning()(implicit
      context: E#ActorContextT[Pruning.Message],
      traceContext: TraceContext,
  ): Unit = {
    logger.info(s"Scheduling pruning in ${config.pruningFrequency}")
    periodicPruningCancellable.foreach(_.cancel())
    periodicPruningCancellable = Some(
      context.delayedEvent(
        config.pruningFrequency,
        Pruning.KickstartPruning(config.retentionPeriod, config.minNumberOfBlocksToKeep, None),
      )
    )
  }
}
