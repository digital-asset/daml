// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import akka.stream.ActorMaterializer
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.engine.{Error => LfError}
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.daml.lf.transaction.Transaction.Transaction
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.domain.Commands
import com.digitalasset.ledger.api.messages.command.submission.SubmitRequest
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceLogging
import com.digitalasset.ledger.backend.api.v1.{LedgerBackend, SubmissionResult}
import com.digitalasset.platform.participant.util.ApiToLfEngine.apiCommandsToLfCommands
import com.digitalasset.platform.sandbox.config.DamlPackageContainer
import com.digitalasset.platform.sandbox.stores.ledger.{CommandExecutor, ErrorCause}
import com.digitalasset.platform.server.api.services.domain.CommandSubmissionService
import com.digitalasset.platform.server.api.services.grpc.GrpcCommandSubmissionService
import com.digitalasset.platform.server.api.validation.{ErrorFactories, IdentifierResolver}
import com.digitalasset.platform.server.services.command.time.TimeModelValidator
import com.digitalasset.platform.services.time.TimeModel
import io.grpc.{BindableService, Status}
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object SandboxSubmissionService {

  type RecordUpdate = Either[LfError, (Transaction, BlindingInfo)]

  def createApiService(
      packageContainer: DamlPackageContainer,
      identifierResolver: IdentifierResolver,
      ledgerBackend: LedgerBackend,
      timeModel: TimeModel,
      timeProvider: TimeProvider,
      commandExecutor: CommandExecutor)(implicit ec: ExecutionContext, mat: ActorMaterializer)
    : GrpcCommandSubmissionService with BindableService with CommandSubmissionServiceLogging =
    new GrpcCommandSubmissionService(
      new SandboxSubmissionService(
        packageContainer,
        ledgerBackend,
        timeModel,
        timeProvider,
        commandExecutor),
      ledgerBackend.ledgerId,
      identifierResolver
    ) with CommandSubmissionServiceLogging

  object RecordUpdate {
    def apply(views: Either[LfError, (Transaction, BlindingInfo)]): RecordUpdate = views
  }

}

class SandboxSubmissionService private (
    packageContainer: DamlPackageContainer,
    ledgerBackend: LedgerBackend,
    timeModel: TimeModel,
    timeProvider: TimeProvider,
    commandExecutor: CommandExecutor)(implicit ec: ExecutionContext, mat: ActorMaterializer)
    extends CommandSubmissionService
    with ErrorFactories
    with AutoCloseable {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val validator = TimeModelValidator(timeModel)

  override def submit(request: SubmitRequest): Future[Unit] = {
    val commands = request.commands
    val validation = for {
      _ <- validator.checkTtl(commands.ledgerEffectiveTime, commands.maximumRecordTime)
      _ <- validator
        .checkLet(
          timeProvider.getCurrentTime,
          commands.ledgerEffectiveTime,
          commands.maximumRecordTime,
          commands.commandId.unwrap,
          commands.applicationId.unwrap)
    } yield ()

    validation.fold(
      Future.failed,
      _ => {
        logger.debug(
          "Received composite command {} with let {}.",
          if (logger.isTraceEnabled()) commands else commands.commandId.unwrap,
          commands.ledgerEffectiveTime: Any)
        recordOnLedger(commands).transform {
          case Failure(error) =>
            logger.warn(s"Submission of command ${commands.commandId.unwrap} has failed.", error)
            // quick work-around to handle back-pressure coming from Akka
            //TODO: we should encapsulate the back pressure signal in a proper type and make recordOnLedger return a Future[OK \/ BackPressure]
            if (error.getMessage.startsWith(
                "You have to wait for previous offer to be resolved to send another request")) {
              Failure(Status.RESOURCE_EXHAUSTED.asRuntimeException())
            } else
              Failure(error)
          case Success(v) =>
            logger.debug(s"Submission of command {} has succeeded", commands.commandId.unwrap)
            Success(())
        }(DirectExecutionContext)
      }
    )
  }

  private def recordOnLedger(commands: Commands): Future[SubmissionResult] =
    // translate the commands to LF engine commands
    apiCommandsToLfCommands(commands)
      .consume(packageContainer.getPackage)
      .fold(
        e => {
          logger.error(s"Could not translate commands: $e")
          Future.failed(invalidArgument(s"Could not translate command: $e"))
        }, { lfCommands =>
          ledgerBackend.beginSubmission
            .flatMap { handle =>
              commandExecutor
                .execute(
                  commands.submitter,
                  commands,
                  handle.lookupActiveContract,
                  handle.lookupContractKey,
                  lfCommands)
                .flatMap {
                  _.left
                    .map(ec => grpcError(toStatus(ec)))
                    .toTry
                    .fold(Future.failed, handle.submit)
                }
            }
        }
      )

  private def toStatus(errorCause: ErrorCause) = {
    errorCause match {
      case e @ ErrorCause.DamlLf(d) =>
        Status.INVALID_ARGUMENT.withDescription(e.explain)
      case e @ ErrorCause.Sequencer(errors) =>
        val base = if (errors.exists(_.isFinal)) Status.INVALID_ARGUMENT else Status.ABORTED
        base.withDescription(e.explain)
    }
  }

  override def close(): Unit = ()

}
