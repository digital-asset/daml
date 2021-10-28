// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.benchtool.infrastructure.TestDars
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.util.SimpleFileReader
import com.daml.ledger.api.v1.commands.{Command, Commands}
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import java.io.File
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

case class CommandSubmitter(services: LedgerApiServices) {
  private val logger = LoggerFactory.getLogger(getClass)

  private val identifierSuffix = f"${System.nanoTime}%x"
  private val applicationId = "benchtool"
  private val workflowId = s"$applicationId-$identifierSuffix"
  private val signatoryName = s"signatory-$identifierSuffix"
  private def observerName(index: Int): String = s"Obs-$index-$identifierSuffix"
  private def commandId(index: Int): String = s"command-$index-$identifierSuffix"
  private def darId(index: Int) = s"submission-dars-$index-$identifierSuffix"

  def submit(
      descriptorFile: File,
      maxInFlightCommands: Int,
  )(implicit ec: ExecutionContext): Future[Unit] =
    (for {
      _ <- Future.successful(logger.info("Generating contracts..."))
      _ <- Future.successful(logger.info(s"Identifier suffix: $identifierSuffix"))
      descriptor <- Future.fromTry(parseDescriptor(descriptorFile))
      signatory <- allocateParty(signatoryName)
      observers <- allocateParties(descriptor.numberOfObservers, observerName)
      _ <- uploadTestDars()
      _ <- submitCommands(descriptor, signatory, observers, maxInFlightCommands)
    } yield logger.info("Commands submitted successfully."))
      .recoverWith { case NonFatal(ex) =>
        logger.error(s"Command submission failed. Details: ${ex.getLocalizedMessage}", ex)
        Future.failed(CommandSubmitter.CommandSubmitterError(ex.getLocalizedMessage))
      }

  private def parseDescriptor(descriptorFile: File): Try[ContractSetDescriptor] =
    SimpleFileReader.readFile(descriptorFile)(DescriptorParser.parse).flatMap {
      case Left(err: DescriptorParser.DescriptorParserError) =>
        val message = s"Descriptor parsing error. Details: ${err.details}"
        logger.error(message)
        Failure(CommandSubmitter.CommandSubmitterError(message))
      case Right(descriptor) =>
        logger.info(s"Descriptor parsed: $descriptor")
        Success(descriptor)
    }

  private def allocateParty(name: String)(implicit ec: ExecutionContext): Future[Primitive.Party] =
    services.partyManagementService.allocateParty(name)

  private def allocateParties(number: Int, name: Int => String)(implicit
      ec: ExecutionContext
  ): Future[List[Primitive.Party]] =
    (1 to number).foldLeft(Future.successful(List.empty[Primitive.Party])) { (allocated, i) =>
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

  private def submitAndWait(id: String, party: Primitive.Party, command: Command)(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    val commands = new Commands(
      ledgerId = services.ledgerId,
      applicationId = applicationId,
      commandId = id,
      party = party.unwrap,
      commands = List(command),
      workflowId = workflowId,
    )
    services.commandService.submitAndWait(commands).map(_ => ())
  }

  private def submitCommands(
      descriptor: ContractSetDescriptor,
      signatory: Primitive.Party,
      observers: List[Primitive.Party],
      maxInFlightCommands: Int,
  )(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    implicit val resourceContext: ResourceContext = ResourceContext(ec)

    val progressMeter = CommandSubmitter.ProgressMeter(descriptor.numberOfInstances)
    // Output a log line roughly once per 10% progress, or once every 500 submissions (whichever comes first)
    val progressLogInterval = math.min(descriptor.numberOfInstances / 10 + 1, 500)
    val progressLoggingSink =
      Sink.foreach[Int](index =>
        if (index % progressLogInterval == 0) {
          logger.info(progressMeter.getProgress(index))
        }
      )
    val generator = new CommandGenerator(RandomnessProvider.Default, descriptor, observers)

    logger.info("Submitting commands...")
    materializerOwner()
      .use { implicit materializer =>
        Source
          .fromIterator(() => (1 to descriptor.numberOfInstances).iterator)
          .mapAsync(maxInFlightCommands) { index =>
            generator.next() match {
              case Success(command) =>
                submitAndWait(
                  id = commandId(index),
                  party = signatory,
                  command = command(signatory),
                ).map(_ => index)
              case Failure(ex) =>
                Future.failed {
                  logger.error(s"Command generation failed. Details: ${ex.getLocalizedMessage}", ex)
                  CommandSubmitter.CommandSubmitterError(ex.getLocalizedMessage)
                }
            }
          }
          .runWith(progressLoggingSink)
      }
      .map(_ => ())
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

  class ProgressMeter(totalItems: Int, startTimeMillis: Long) {
    def getProgress(index: Int): String =
      f"Progress: $index/${totalItems} (${percentage(index)}%%). Remaining time: ${remainingSeconds(index)}%1.1f s"

    private def percentage(index: Int): Double = (index.toDouble / totalItems) * 100

    private def remainingSeconds(index: Int): Double = {
      val remainingItems = totalItems - index
      if (remainingItems > 0) {
        val elapsedSeconds: Double = (System.currentTimeMillis() - startTimeMillis).toDouble / 1000
        val timePerItem: Double = elapsedSeconds / index
        remainingItems * timePerItem
      } else {
        0.0
      }
    }
  }

  object ProgressMeter {
    def apply(totalItems: Int) = new ProgressMeter(
      totalItems = totalItems,
      startTimeMillis = System.currentTimeMillis(),
    )
  }
}
