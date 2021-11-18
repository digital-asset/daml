// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import akka.actor.ActorSystem
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.benchtool.config.WorkflowConfig.SubmissionConfig
import com.daml.ledger.api.benchtool.infrastructure.TestDars
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.v1.commands.{Command, Commands}
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

case class CommandSubmitter(services: LedgerApiServices) {
  private val logger = LoggerFactory.getLogger(getClass)

  private val identifierSuffix = f"${System.nanoTime}%x"
  private val applicationId = "benchtool"
  private val workflowId = s"$applicationId-$identifierSuffix"
  private val signatoryName = s"signatory-$identifierSuffix"
  private def observerName(index: Int, uniqueParties: Boolean): String = {
    if (uniqueParties) s"Obs-$index-$identifierSuffix"
    else s"Obs-$index"
  }
  private def commandId(index: Int): String = s"command-$index-$identifierSuffix"
  private def darId(index: Int) = s"submission-dars-$index-$identifierSuffix"

  def submit(
      config: SubmissionConfig,
      maxInFlightCommands: Int,
      submissionBatchSize: Int,
  )(implicit ec: ExecutionContext): Future[CommandSubmitter.SubmissionSummary] =
    (for {
      _ <- Future.successful(logger.info("Generating contracts..."))
      _ <- Future.successful(logger.info(s"Identifier suffix: $identifierSuffix"))
      signatory <- allocateParty(signatoryName)
      observers <- allocateParties(
        number = config.numberOfObservers,
        name = index => observerName(index, config.uniqueParties),
      )
      _ <- uploadTestDars()
      _ <- submitCommands(
        config,
        signatory,
        observers,
        maxInFlightCommands,
        submissionBatchSize,
      )
    } yield {
      logger.info("Commands submitted successfully.")
      CommandSubmitter.SubmissionSummary(
        observers = observers
      )
    })
      .recoverWith { case NonFatal(ex) =>
        logger.error(s"Command submission failed. Details: ${ex.getLocalizedMessage}", ex)
        Future.failed(CommandSubmitter.CommandSubmitterError(ex.getLocalizedMessage))
      }

  private def allocateParty(name: String)(implicit ec: ExecutionContext): Future[Primitive.Party] =
    services.partyManagementService.allocateParty(name)

  private def allocateParties(number: Int, name: Int => String)(implicit
      ec: ExecutionContext
  ): Future[List[Primitive.Party]] =
    (0 until number).foldLeft(Future.successful(List.empty[Primitive.Party])) { (allocated, i) =>
      allocated.flatMap { parties =>
        services.partyManagementService.allocateParty(name(i)).map(party => parties :+ party)
      }
    }

  private def uploadDar(dar: TestDars.DarFile, submissionId: String)(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    services.packageManagementService.uploadDar(
      bytes = dar.bytes,
      submissionId = submissionId,
    )

  private def uploadTestDars()(implicit ec: ExecutionContext): Future[Unit] =
    for {
      dars <- Future.fromTry(TestDars.readAll())
      _ <- Future.sequence {
        dars.zipWithIndex
          .map { case (dar, index) =>
            uploadDar(dar, darId(index))
          }
      }
    } yield ()

  private def submitAndWait(id: String, party: Primitive.Party, commands: List[Command])(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    val result = new Commands(
      ledgerId = services.ledgerId,
      applicationId = applicationId,
      commandId = id,
      party = party.unwrap,
      commands = commands,
      workflowId = workflowId,
    )
    services.commandService.submitAndWait(result).map(_ => ())
  }

  private def submitCommands(
      config: SubmissionConfig,
      signatory: Primitive.Party,
      observers: List[Primitive.Party],
      maxInFlightCommands: Int,
      submissionBatchSize: Int,
  )(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    implicit val resourceContext: ResourceContext = ResourceContext(ec)

    val numBatches: Int = config.numberOfInstances / submissionBatchSize
    val progressMeter = CommandSubmitter.ProgressMeter(config.numberOfInstances)
    // Output a log line roughly once per 10% progress, or once every 500 submissions (whichever comes first)
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

    val generator = new CommandGenerator(
      randomnessProvider = RandomnessProvider.Default,
      signatory = signatory,
      config = config,
      observers = observers,
    )

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
              submitAndWait(
                id = commandId(index),
                party = signatory,
                commands = commands,
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
      f"Progress: $index/${totalItems} (${percentage(index)}%1.1f%%). Elapsed time: ${elapsedSeconds}%1.1f s. Remaining time: ${remainingSeconds(index)}%1.1f s"

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
