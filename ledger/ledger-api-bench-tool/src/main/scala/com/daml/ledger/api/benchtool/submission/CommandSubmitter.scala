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
    metricRegistry: MetricRegistry,
    metricsManager: MetricsManager[LatencyNanos],
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val submitAndWaitTimer = metricRegistry.timer("daml_submit_and_wait_latency")

  def prepare(config: SubmissionConfig)(implicit
      ec: ExecutionContext
  ): Future[
    (
        client.binding.Primitive.Party,
        List[client.binding.Primitive.Party],
        List[client.binding.Primitive.Party],
    )
  ] = {
    val observerPartyNames =
      names.observerPartyNames(config.numberOfObservers, config.uniqueParties)
    val divulgeePartyNames =
      names.divulgeePartyNames(config.numberOfDivulgees, config.uniqueParties)

    logger.info("Generating contracts...")
    logger.info(s"Identifier suffix: ${names.identifierSuffix}")
    (for {
      signatory <- allocateSignatoryParty()
      observers <- allocateParties(observerPartyNames)
      divulgees <- allocateParties(divulgeePartyNames)
      _ <- uploadTestDars()
    } yield {
      logger.info("Prepared command submission.")
      (signatory, observers, divulgees)
    })
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
    submitAndWait(
      id = commandId,
      actAs = actAs,
      commands = commands,
    )
  }

  def generateAndSubmit(
      generator: CommandGenerator,
      config: SubmissionConfig,
      actAs: List[client.binding.Primitive.Party],
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
        actAs = actAs,
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

  private def allocateSignatoryParty()(implicit ec: ExecutionContext): Future[Primitive.Party] =
    adminServices.partyManagementService.allocateParty(names.signatoryPartyName)

  private def allocateParties(divulgeePartyNames: Seq[String])(implicit
      ec: ExecutionContext
  ): Future[List[Primitive.Party]] = {
    Future.sequence(
      divulgeePartyNames.toList.map(adminServices.partyManagementService.allocateParty)
    )
  }

  private def uploadDar(dar: TestDars.DarFile, submissionId: String)(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    adminServices.packageManagementService.uploadDar(
      bytes = dar.bytes,
      submissionId = submissionId,
    )

  private def uploadTestDars()(implicit ec: ExecutionContext): Future[Unit] =
    for {
      dars <- Future.fromTry(TestDars.readAll())
      _ <- Future.sequence {
        dars.zipWithIndex
          .map { case (dar, index) =>
            uploadDar(dar, names.darId(index))
          }
      }
    } yield ()

  private def submitAndWait(
      id: String,
      actAs: Seq[Primitive.Party],
      commands: Seq[Command],
  )(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    def makeCommands(commands: Seq[Command]) = new Commands(
      ledgerId = benchtoolUserServices.ledgerId,
      applicationId = names.benchtoolApplicationId,
      commandId = id,
      actAs = actAs.map(_.unwrap),
      commands = commands,
      workflowId = names.workflowId,
    )

    for {
      _ <- benchtoolUserServices.commandService
        .submitAndWait(makeCommands(commands))
    } yield ()
  }

  private def submitCommands(
      generator: CommandGenerator,
      config: SubmissionConfig,
      actAs: List[Primitive.Party],
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
            .mapAsync(8)(index =>
              Future.fromTry(
                generator.next().map(cmd => index -> cmd)
              )
            )
            .groupedWithin(submissionBatchSize, 1.minute)
            .map(cmds => cmds.head._1 -> cmds.map(_._2).toList)
            .buffer(maxInFlightCommands, OverflowStrategy.backpressure)
            .mapAsync(maxInFlightCommands) { case (index, commands) =>
              timed(submitAndWaitTimer, metricsManager)(
                submitAndWait(
                  id = names.commandId(index),
                  actAs = actAs,
                  commands = commands.flatten,
                )
              )
                .map(_ => index + commands.length - 1)
                .recoverWith { case ex =>
                  Future.failed {
                    logger.error(
                      s"Command submission failed. Details: ${ex.getLocalizedMessage}",
                      ex,
                    )
                    CommandSubmitter.CommandSubmitterError(ex.getLocalizedMessage)
                  }
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
