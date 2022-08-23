// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{Materializer, OverflowStrategy}
import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.ledger.api.benchtool.config.WorkflowConfig.SubmissionConfig
import com.daml.ledger.api.benchtool.infrastructure.TestDars
import com.daml.ledger.api.benchtool.metrics.LatencyMetric.LatencyNanos
import com.daml.ledger.api.benchtool.metrics.MetricsManager
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.v1.commands.{Command, Commands}
import com.daml.ledger.client
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import io.grpc.Status
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._
import scala.util.control.NonFatal

case class CommandSubmitter(
    names: Names,
    benchtoolUserServices: LedgerApiServices,
    adminServices: LedgerApiServices,
    partyAllocating: PartyAllocating,
    metricRegistry: MetricRegistry,
    metricsManager: MetricsManager[LatencyNanos],
    waitForSubmission: Boolean,
    commandGenerationParallelism: Int = 8,
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val submitLatencyTimer = if (waitForSubmission) {
    metricRegistry.timer("daml_submit_and_wait_latency")
  } else {
    metricRegistry.timer("daml_submit_latency")
  }

  def prepare(config: SubmissionConfig)(implicit
      ec: ExecutionContext
  ): Future[AllocatedParties] = {
    logger.info(s"Identifier suffix: ${names.identifierSuffix}")
    (for {
      allocatedParties <- partyAllocating.allocateParties(config)
      _ <- uploadTestDars()
    } yield allocatedParties)
      .recoverWith { case NonFatal(ex) =>
        logger.error(
          s"Command submission preparation failed. Details: ${ex.getLocalizedMessage}",
          ex,
        )
        Future.failed(CommandSubmitter.CommandSubmitterError(ex.getLocalizedMessage))
      }
  }

  def submitSingleBatch(
      commandId: String,
      actAs: Seq[Primitive.Party],
      commands: Seq[Command],
  )(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    submit(
      id = commandId,
      actAs = actAs,
      commands = commands,
      applicationId = names.benchtoolApplicationId,
      useSubmitAndWait = true,
    )
  }

  def generateAndSubmit(
      generator: CommandGenerator,
      config: SubmissionConfig,
      baseActAs: List[client.binding.Primitive.Party],
      maxInFlightCommands: Int,
      submissionBatchSize: Int,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info("Generating contracts...")
    (for {
      _ <- submitCommands(
        generator = generator,
        config = config,
        maxInFlightCommands = maxInFlightCommands,
        submissionBatchSize = submissionBatchSize,
        baseActAs = baseActAs,
      )
    } yield {
      logger.info("Commands submitted successfully.")
      ()
    })
      .recoverWith { case NonFatal(ex) =>
        logger.error(s"Command submission failed. Details: ${ex.getLocalizedMessage}", ex)
        Future.failed(CommandSubmitter.CommandSubmitterError(ex.getLocalizedMessage))
      }
  }

  private def uploadDar(dar: TestDars.DarFile, submissionId: String)(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    adminServices.packageManagementService.uploadDar(
      bytes = dar.bytes,
      submissionId = submissionId,
    )

  private def uploadTestDars()(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info("Uploading dars...")
    for {
      dars <- Future.delegate { Future.fromTry(TestDars.readAll()) }
      _ <- Future.sequence {
        dars.zipWithIndex
          .map { case (dar, index) =>
            uploadDar(dar, names.darId(index))
          }
      }
    } yield {
      logger.info("Uplading dars completed")
    }
  }

  private def submit(
      id: String,
      actAs: Seq[Primitive.Party],
      commands: Seq[Command],
      applicationId: String,
      useSubmitAndWait: Boolean,
  )(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    def makeCommands(commands: Seq[Command]) = new Commands(
      ledgerId = benchtoolUserServices.ledgerId,
      applicationId = applicationId,
      commandId = id,
      actAs = actAs.map(_.unwrap),
      commands = commands,
      workflowId = names.workflowId,
    )

    (if (useSubmitAndWait) {
       benchtoolUserServices.commandService.submitAndWait(makeCommands(commands))
     } else {
       benchtoolUserServices.commandSubmissionService.submit(makeCommands(commands))
     }).map(_ => ())
  }

  private def submitCommands(
      generator: CommandGenerator,
      config: SubmissionConfig,
      baseActAs: List[Primitive.Party],
      maxInFlightCommands: Int,
      submissionBatchSize: Int,
  )(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    implicit val resourceContext: ResourceContext = ResourceContext(ec)

    val numBatches: Int = config.numberOfInstances / submissionBatchSize
    val progressMeter = CommandSubmitter.ProgressMeter(config.numberOfInstances)
    // Output a log line roughly once per 10% progress, or once every 10000 submissions (whichever comes first)
    val progressLogInterval = math.min(config.numberOfInstances / 10 + 1, 10000)
    val progressLoggingSink = {
      var lastInterval = 0
      Sink.foreach[Int](index =>
        if (index / progressLogInterval != lastInterval) {
          lastInterval = index / progressLogInterval
          logger.info(progressMeter.getProgress(index))
        }
      )

    }
    logger.info(
      s"Submitting commands ($numBatches commands, $submissionBatchSize contracts per command)..."
    )
    materializerOwner()
      .use { implicit materializer =>
        for {
          _ <- Source
            .fromIterator(() => (1 to config.numberOfInstances).iterator)
            .wireTap(i => if (i == 1) progressMeter.start())
            .mapAsync(commandGenerationParallelism)(index =>
              Future.fromTry(
                generator.next().map(cmd => index -> cmd)
              )
            )
            .groupedWithin(submissionBatchSize, 1.minute)
            .map(cmds => cmds.head._1 -> cmds.map(_._2).toList)
            .buffer(maxInFlightCommands, OverflowStrategy.backpressure)
            .mapAsync(maxInFlightCommands) { case (index, commands) =>
              timed(submitLatencyTimer, metricsManager) {
                submit(
                  id = names.commandId(index),
                  actAs = baseActAs ++ generator.nextExtraCommandSubmitters(),
                  commands = commands.flatten,
                  applicationId = generator.nextApplicationId(),
                  useSubmitAndWait = config.waitForSubmission,
                )
              }
                .map(_ => index + commands.length - 1)
                .recoverWith {
                  case e: io.grpc.StatusRuntimeException
                      if e.getStatus.getCode == Status.Code.ABORTED =>
                    logger.info(s"Flow rate limited at index $index: ${e.getLocalizedMessage}")
                    Thread.sleep(10) // Small back-off period
                    Future.successful(index + commands.length - 1)
                  case ex =>
                    logger.error(
                      s"Command submission failed. Details: ${ex.getLocalizedMessage}",
                      ex,
                    )
                    Future.failed(CommandSubmitter.CommandSubmitterError(ex.getLocalizedMessage))
                }
            }
            .runWith(progressLoggingSink)
        } yield ()
      }
  }

  private def materializerOwner(): ResourceOwner[Materializer] = {
    for {
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem("CommandSubmissionSystem"))
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
    } yield materializer
  }

  private def timed[O](timer: Timer, metricsManager: MetricsManager[LatencyNanos])(
      f: => Future[O]
  )(implicit ec: ExecutionContext) = {
    val ctx = timer.time()
    f.map(_.tap { _ =>
      val latencyNanos = ctx.stop()
      metricsManager.sendNewValue(latencyNanos)
    })
  }
}

object CommandSubmitter {
  case class CommandSubmitterError(msg: String) extends RuntimeException(msg)

  case class SubmissionSummary(observers: List[Primitive.Party])

  class ProgressMeter(totalItems: Int) {
    var startTimeMillis: Long = System.currentTimeMillis()

    def start(): Unit = {
      startTimeMillis = System.currentTimeMillis()
    }

    def getProgress(index: Int): String =
      f"Progress: $index/$totalItems (${percentage(index)}%1.1f%%). Elapsed time: ${elapsedSeconds}%1.1f s. Remaining time: ${remainingSeconds(index)}%1.1f s"

    private def percentage(index: Int): Double = (index.toDouble / totalItems) * 100

    private def elapsedSeconds: Double =
      (System.currentTimeMillis() - startTimeMillis).toDouble / 1000

    private def remainingSeconds(index: Int): Double = {
      val remainingItems = totalItems - index
      if (remainingItems > 0) {
        val timePerItem: Double = elapsedSeconds / index
        remainingItems * timePerItem
      } else {
        0.0
      }
    }
  }

  object ProgressMeter {
    def apply(totalItems: Int) = new ProgressMeter(
      totalItems = totalItems
    )
  }
}
