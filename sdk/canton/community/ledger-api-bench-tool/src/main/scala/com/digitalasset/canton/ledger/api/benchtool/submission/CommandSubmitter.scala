// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod.Empty
import com.daml.ledger.api.v2.commands.{Command, Commands}
import com.daml.ledger.javaapi.data.Party
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.metrics.api.MetricHandle.{LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.SubmissionConfig
import com.digitalasset.canton.ledger.api.benchtool.infrastructure.TestDars
import com.digitalasset.canton.ledger.api.benchtool.metrics.LatencyMetric.LatencyNanos
import com.digitalasset.canton.ledger.api.benchtool.metrics.MetricsManager
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import io.grpc.Status
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.{Materializer, OverflowStrategy}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*
import scala.util.control.NonFatal

final case class CommandSubmitter(
    names: Names,
    benchtoolUserServices: LedgerApiServices,
    adminServices: LedgerApiServices,
    partyAllocating: PartyAllocating,
    metricsFactory: LabeledMetricsFactory,
    metricsManager: MetricsManager[LatencyNanos],
    waitForSubmission: Boolean,
    commandGenerationParallelism: Int = 8,
    maxInFlightCommandsOverride: Option[Int] = None,
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val submitLatencyTimer = if (waitForSubmission) {
    metricsFactory.timer(
      MetricInfo(
        MetricName("daml_submit_and_wait_latency"),
        "Submit and wait latency",
        MetricQualification.Debug,
      )
    )
  } else {
    metricsFactory.timer(
      MetricInfo(MetricName("daml_submit_latency"), "Submit latency", MetricQualification.Debug)
    )
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
        Future.failed(CommandSubmitter.CommandSubmitterError(ex.getLocalizedMessage, ex))
      }
  }

  def submitSingleBatch(
      commandId: String,
      actAs: Seq[Party],
      commands: Seq[Command],
  )(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    submit(
      id = commandId,
      actAs = actAs,
      commands = commands,
      userId = names.benchtoolUserId,
      useSubmitAndWait = true,
    )

  def generateAndSubmit(
      generator: CommandGenerator,
      config: SubmissionConfig,
      baseActAs: List[Party],
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
        Future.failed(CommandSubmitter.CommandSubmitterError(ex.getLocalizedMessage, ex))
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
      dars <- Future.delegate(Future.fromTry(TestDars.readAll()))
      _ <- Future.sequence {
        dars.zipWithIndex
          .map { case (dar, index) =>
            uploadDar(dar, names.mainPackageId(index))
          }
      }
    } yield {
      logger.info("Uplading dars completed")
    }
  }

  private def submit(
      id: String,
      actAs: Seq[Party],
      commands: Seq[Command],
      userId: String,
      useSubmitAndWait: Boolean,
  )(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    def makeCommands(commands: Seq[Command]) =
      new Commands(
        userId = userId,
        commandId = id,
        actAs = actAs.map(_.getValue),
        commands = commands,
        workflowId = names.workflowId,
        minLedgerTimeAbs = None,
        minLedgerTimeRel = None,
        readAs = Nil,
        submissionId = "",
        disclosedContracts = Nil,
        synchronizerId = "",
        packageIdSelectionPreference = Nil,
        prefetchContractKeys = Nil,
        deduplicationPeriod = Empty,
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
      baseActAs: List[Party],
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
    val progressLoggingSink =
      Sink.fold[Int, Int](0)((lastInterval, index) =>
        if (index / progressLogInterval != lastInterval) {
          logger.info(progressMeter.getProgress(index))
          index / progressLogInterval
        } else
          lastInterval
      )

    logger.info(
      s"Submitting commands ($numBatches commands, $submissionBatchSize contracts per command)..."
    )
    materializerOwner()
      .use { implicit materializer =>
        for {
          _ <- generator
            .commandBatchSource(config.numberOfInstances, commandGenerationParallelism)
            .groupedWithin(submissionBatchSize, 1.minute)
            .map(cmds => cmds.headOption.map(cmd => cmd._1 -> cmds.map(_._2).toList))
            .buffer(maxInFlightCommands, OverflowStrategy.backpressure)
            .mapAsync(maxInFlightCommandsOverride.getOrElse(maxInFlightCommands)) {
              case Some((index, commands)) =>
                timed(submitLatencyTimer, metricsManager) {
                  submit(
                    id = names.commandId(index),
                    actAs = baseActAs ++ generator.nextExtraCommandSubmitters(),
                    commands = commands.flatten,
                    userId = generator.nextUserId(),
                    useSubmitAndWait = config.waitForSubmission,
                  )
                }
                  .map(_ => index + commands.length - 1)
                  .recoverWith {
                    case e: io.grpc.StatusRuntimeException
                        if e.getStatus.getCode == Status.Code.ABORTED =>
                      logger.info(s"Flow rate limited at index $index: ${e.getLocalizedMessage}")
                      Threading.sleep(10) // Small back-off period
                      Future.successful(index + commands.length - 1)
                    case ex =>
                      logger.error(
                        s"Command submission failed. Details: ${ex.getLocalizedMessage}",
                        ex,
                      )
                      Future.failed(
                        CommandSubmitter.CommandSubmitterError(ex.getLocalizedMessage, ex)
                      )
                  }
              // Impossible, because otherwise division by zero would have thrown an exception earlier.
              case None => Future.failed(new NoSuchElementException("Empty command group"))
            }
            .runWith(progressLoggingSink)
        } yield ()
      }
  }

  private def materializerOwner(): ResourceOwner[Materializer] =
    for {
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem("CommandSubmissionSystem"))
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
    } yield materializer

  private def timed[O](timer: Timer, metricsManager: MetricsManager[LatencyNanos])(
      f: => Future[O]
  )(implicit ec: ExecutionContext) = {
    val ctx = timer.startAsync()
    val startNanos = System.nanoTime()
    f.map(_.tap { _ =>
      ctx.stop()
      val endNanos = System.nanoTime()
      metricsManager.sendNewValue(endNanos - startNanos)
    })
  }
}

object CommandSubmitter {
  final case class CommandSubmitterError(msg: String, cause: Throwable)
      extends RuntimeException(msg, cause)

  final case class SubmissionSummary(observers: List[Party])

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  class ProgressMeter(totalItems: Int) {
    var startTimeMillis: Long = System.currentTimeMillis()

    def start(): Unit =
      startTimeMillis = System.currentTimeMillis()

    def getProgress(index: Int): String =
      f"Progress: $index/$totalItems (${percentage(index)}%1.1f%%). Elapsed time: $elapsedSeconds%1.1f s. Remaining time: ${remainingSeconds(index)}%1.1f s"

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
