// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.api.benchtool.infrastructure.TestDars
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.util.SimpleFileReader
import com.daml.ledger.api.v1.commands.{Command, Commands}
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.client.binding.Primitive.Party
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
  private def commandId(index: Int) = s"command-$index-$identifierSuffix"

  def submitCommands(
      descriptorFile: File
  )(implicit ec: ExecutionContext): Future[Unit] =
    (for {
      _ <- Future.successful(logger.info("Generating contracts..."))
      descriptor <- Future.fromTry(parseDescriptor(descriptorFile))
      party <- allocateParty()
      _ <- uploadTestDars()
      _ <- createContracts(descriptor = descriptor, party = party)
    } yield logger.info("Contracts produced successfully.")).recoverWith { case NonFatal(ex) =>
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

  private def allocateParty()(implicit ec: ExecutionContext): Future[Primitive.Party] =
    services.partyManagementService.allocateParty(
      hint = s"party-0-$identifierSuffix"
    )

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
      _ <- Future.sequence(dars.zipWithIndex.map { case (dar, index) =>
        uploadDar(dar, s"submission-dars-$index-$identifierSuffix")
      })
    } yield ()

  private def createContract(id: String, party: Party, createCommand: Command)(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    val commands = new Commands(
      ledgerId = services.ledgerId,
      applicationId = applicationId,
      commandId = id,
      party = party.unwrap,
      commands = List(createCommand),
      workflowId = workflowId,
    )
    services.commandService.submitAndWait(commands).map(_ => ())
  }

  private def createContracts(descriptor: ContractSetDescriptor, party: Party)(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    def logProgress(index: Int): Unit =
      if (index % 100 == 0) {
        logger.info(
          s"Created contracts: $index out of ${descriptor.numberOfInstances} (${(index.toDouble / descriptor.numberOfInstances) * 100}%)"
        )
      }

    val generator = new CommandGenerator(RandomnessProvider.Default, descriptor, party)

    implicit val resourceContext: ResourceContext = ResourceContext(ec)
    materializerOwner()
      .use { implicit materializer =>
        Source
          .fromIterator(() => (1 to descriptor.numberOfInstances).iterator)
          .mapAsync(100) { index =>
            generator.next() match {
              case Right(command) =>
                createContract(
                  id = commandId(index),
                  party = party,
                  createCommand = command(party),
                ).map(_ => index)
              case Left(error) =>
                Future.failed {
                  logger.error(s"Command generation failed. Details: $error")
                  CommandSubmitter.CommandSubmitterError(error)
                }
            }
          }
          .runForeach(logProgress)
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
}
