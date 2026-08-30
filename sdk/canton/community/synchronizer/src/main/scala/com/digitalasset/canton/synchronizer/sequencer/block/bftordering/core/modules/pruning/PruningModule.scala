// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.scheduler.{BftOrdererPruningCronSchedule, JobScheduler}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Pruning
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Pruning.OngoingPruningOperation
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  CancellableEvent,
  Env,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext

import scala.jdk.DurationConverters.{ScalaDurationOps, *}
import scala.util.{Failure, Success}

final class PruningModule[E <: Env[E]](
    stores: BftOrderingStores[E],
    clock: Clock,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit metricsContext: MetricsContext)
    extends Pruning[E] {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var ongoingOperation: OngoingPruningOperation = OngoingPruningOperation.None
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var periodicPruningCancellable: Option[CancellableEvent] = None
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var pruningSchedule: Option[BftOrdererPruningSchedule] = None

  override def receiveInternal(
      message: Pruning.Message
  )(implicit context: E#ActorContextT[Pruning.Message], traceContext: TraceContext): Unit =
    message match {
      case Pruning.Start =>
        pipeToSelfOpt(stores.outputStore.getLowerBound()) {
          case Success(Some(lowerBound)) =>
            Some(Pruning.PerformPruning(lowerBound.epochNumber))
          case Success(None) =>
            None
          case Failure(exception) =>
            Some(
              Pruning.FailedDatabaseOperation(
                "Failed to fetch initial pruning lower bound",
                exception,
              )
            )
        }
      case Pruning.KickstartPruning(retention, minBlocksToKeep, requestPromise) =>
        def kickStartPruning(): Unit = {
          logger.info(s"Kick-starting new pruning operation")
          pipeToSelfOpt(stores.outputStore.getLastConsecutiveBlock) {
            case Success(Some(block)) =>
              Some(Pruning.ComputePruningPoint(block, retention, minBlocksToKeep))
            case Success(None) =>
              logger.warn("No latest block information available to start pruning")
              completePruningOperation("No latest block information available to start pruning")
            case Failure(exception) =>
              Some(Pruning.FailedDatabaseOperation("Failed to fetch latest block", exception))
          }
        }
        (ongoingOperation, requestPromise) match {
          case (OngoingPruningOperation.None, Some(promise)) =>
            ongoingOperation = OngoingPruningOperation.Manual(promise)
            kickStartPruning()
          case (OngoingPruningOperation.None, None) =>
            ongoingOperation = OngoingPruningOperation.Scheduled
            kickStartPruning()
          case (_, Some(promise)) =>
            promise.complete(
              Success("Pruning won't continue because an operation is already taking place")
            )
          case (_, None) =>
            logger.debug(
              "A scheduled pruning operation could not start because there is an ongoing pruning operation. Rescheduling."
            )
            pruningSchedule.foreach(schedule => schedulePruning(schedule))
        }
      case Pruning.ComputePruningPoint(latestBlock, retention, minBlocksToKeep) =>
        logger.debug(s"Computing new pruning point from latest block $latestBlock")

        val pruningTimestamp = latestBlock.blockBftTime.minus(retention.toJava)
        val minBlockToKeep = BlockNumber(latestBlock.blockNumber - minBlocksToKeep)

        pipeToSelfOpt(
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
                Some(Pruning.SaveNewLowerBound(epoch))
              case (Some(_), None) =>
                // Pruning cannot be performed in this case, otherwise we would end up with
                // fewer blocks than the minimum number of blocks to keep.
                val msg =
                  s"Pruning can't be performed because there are not enough blocks to keep the minimum amount of blocks $minBlocksToKeep"
                logger.debug(msg)
                completePruningOperation(msg)
              case (None, Some(_)) =>
                // Pruning cannot be performed in this case, otherwise we would end up pruning blocks inside the
                // retention period allows.
                val msg =
                  s"Pruning can't be performed because there are no blocks to be pruned outside the retention period of $retention"
                logger.debug(msg)
                completePruningOperation(msg)
              case (None, None) =>
                // Could happen if there are not enough blocks yet, so there are no blocks before the pruning point.
                val msg = s"No pruning point found from latest block ${latestBlock.blockNumber}"
                logger.debug(msg)
                completePruningOperation(msg)
            }
          case Failure(exception) =>
            Some(
              Pruning.FailedDatabaseOperation(
                "Failed to fetch pruning point from database",
                exception,
              )
            )
        }
      case Pruning.SaveNewLowerBound(epochNumber) =>
        logger.debug(s"Saving new lower bound $epochNumber")
        pipeToSelfOpt(stores.outputStore.saveLowerBound(epochNumber)) {
          case Success(Right(())) => Some(Pruning.PerformPruning(epochNumber))
          case Success(Left(error)) =>
            val msg = s"Failed to save new pruning lower bound: $error"
            logger.error(msg)
            completePruningOperation(msg)
          case Failure(exception) =>
            Some(
              Pruning.FailedDatabaseOperation(
                "Failed to persist new pruning lower bound",
                exception,
              )
            )
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
        pipeToSelfOpt(pruneFuture) {
          case Success(
                (outputStorePrunedRecords, epochStorePrunedRecords, availabilityStorePrunedRecords)
              ) =>
            val msg = s"""|Pruning at epoch $epochNumber complete.
                          |EpochStore: pruned ${epochStorePrunedRecords.epochs} epochs, ${epochStorePrunedRecords.pbftMessagesCompleted} pbft messages.
                          |OutputStore: pruned ${outputStorePrunedRecords.epochs} epochs and ${outputStorePrunedRecords.blocks} blocks.
                          |AvailabilityStore: pruned ${availabilityStorePrunedRecords.batches} batches.""".stripMargin
            logger.info(msg)
            completePruningOperation(msg)
          case Failure(exception) =>
            Some(Pruning.FailedDatabaseOperation("Failed to perform pruning", exception))
        }
      case Pruning.FailedDatabaseOperation(msg, exception) =>
        logger.error(msg, exception)
        completePruningOperation(msg).discard
        pruningSchedule.foreach(schedule => schedulePruning(schedule))
      case Pruning.SchedulePruning =>
        pruningSchedule.foreach(schedule => schedulePruning(schedule))

      case Pruning.PruningStatusRequest(promise) =>
        pipeToSelfOpt(
          context.zipFuture(
            stores.outputStore.getLowerBound(),
            stores.outputStore.getLastConsecutiveBlock,
          )
        ) { result =>
          (result match {
            case Success((lowerBound, latestBlock)) =>
              promise.complete(
                Success(
                  BftPruningStatus(
                    latestBlockNumber = latestBlock.fold(BlockNumber.First)(_.blockNumber),
                    latestBlockEpochNumber = latestBlock.fold(EpochNumber.First)(_.epochNumber),
                    latestBlockTimestamp = latestBlock.fold(CantonTimestamp.Epoch)(_.blockBftTime),
                    lowerBoundEpochNumber = lowerBound.fold(EpochNumber.First)(_.epochNumber),
                    lowerBoundBlockNumber = lowerBound.fold(BlockNumber.First)(_.blockNumber),
                  )
                )
              )
            case Failure(exception) => promise.failure(exception)
          }).discard
          None
        }

      case Pruning.StartPruningSchedule(schedule) => startPruningSchedule(schedule)
      case Pruning.CancelPruningSchedule => cancelPruningSchedule()

    }
  private def completePruningOperation(message: String): Option[Pruning.Message] = {
    val result = ongoingOperation match {
      case OngoingPruningOperation.Manual(promise) =>
        promise.complete(Success(message))
        None
      case OngoingPruningOperation.Scheduled =>
        Some(Pruning.SchedulePruning)
      case _ => None
    }
    ongoingOperation = OngoingPruningOperation.None
    result
  }

  private def cancelPruningSchedule()(implicit traceContext: TraceContext): Unit = {
    logger.info(s"Cancelling pruning schedule")
    periodicPruningCancellable.foreach(_.cancel())
    periodicPruningCancellable = None
    pruningSchedule = None
  }

  private def startPruningSchedule(
      schedule: BftOrdererPruningSchedule
  )(implicit
      traceContext: TraceContext,
      context: E#ActorContextT[Pruning.Message],
  ): Unit = {
    pruningSchedule = Some(schedule)
    schedulePruning(schedule)
  }

  private def schedulePruning(bftPruningSchedule: BftOrdererPruningSchedule)(implicit
      context: E#ActorContextT[Pruning.Message],
      traceContext: TraceContext,
  ): Unit = {
    periodicPruningCancellable.foreach(_.cancel())

    val schedule = bftPruningSchedule.schedule
    val cron = new BftOrdererPruningCronSchedule(
      schedule.cron,
      schedule.maxDuration,
      schedule.retention,
      clock,
      logger,
    )

    cron.determineNextRun(JobScheduler.Done) match {
      case Some(nextRun) =>
        val duration = nextRun.waitDuration.duration.toScala
        logger.info(s"Scheduling pruning in $duration based on cron expression ${schedule.cron}")
        periodicPruningCancellable = Some(
          context.delayedEvent(
            duration,
            Pruning.KickstartPruning(
              schedule.retention.toFiniteDuration,
              bftPruningSchedule.minBlocksToKeep,
              None,
            ),
          )
        )
      case None =>
        logger.info(s"Stopping pruning scheduling based on cron expression ${schedule.cron}")
    }
  }
}
