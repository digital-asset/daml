// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceLogging
import akka.stream.ActorMaterializer
import com.daml.ledger.participant.state.v1.SubmissionResult.{Acknowledged, Overloaded}
import com.daml.ledger.participant.state.v1.{
  SubmissionResult,
  SubmitterInfo,
  TransactionMeta,
  WriteService
}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.{Error => LfError}
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.daml.lf.transaction.Transaction.Transaction
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.domain.{LedgerId, Commands => ApiCommands}
import com.digitalasset.ledger.api.messages.command.submission.SubmitRequest
import com.digitalasset.ledger.backend.api.v1.{LedgerBackend, TransactionSubmission}
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

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import com.digitalasset.ledger.api.domain.LedgerId

object SandboxSubmissionService {

  type RecordUpdate = Either[LfError, (Transaction, BlindingInfo)]

  def createApiService(
      ledgerId: LedgerId,
      packageContainer: DamlPackageContainer,
      identifierResolver: IdentifierResolver,
      ledgerBackend: LedgerBackend,
      writeService: WriteService,
      timeModel: TimeModel,
      timeProvider: TimeProvider,
      commandExecutor: CommandExecutor)(implicit ec: ExecutionContext, mat: ActorMaterializer)
    : GrpcCommandSubmissionService with BindableService with CommandSubmissionServiceLogging =
    new GrpcCommandSubmissionService(
      new SandboxSubmissionService(
        packageContainer,
        ledgerBackend,
        writeService,
        timeModel,
        timeProvider,
        commandExecutor),
      ledgerId,
      identifierResolver
    ) with CommandSubmissionServiceLogging

  object RecordUpdate {
    def apply(views: Either[LfError, (Transaction, BlindingInfo)]): RecordUpdate = views
  }

}

class SandboxSubmissionService private (
    packageContainer: DamlPackageContainer,
    ledgerBackend: LedgerBackend,
    writeService: WriteService,
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
          case Success(Acknowledged) =>
            logger.debug(s"Submission of command {} has succeeded", commands.commandId.unwrap)
            Success(())

          case Success(Overloaded) =>
            logger.debug(
              s"Submission of command {} has failed due to back pressure",
              commands.commandId.unwrap)
            Failure(Status.RESOURCE_EXHAUSTED.asRuntimeException)

          case Failure(error) =>
            logger.warn(s"Submission of command ${commands.commandId.unwrap} has failed.", error)
            Failure(error)

        }(DirectExecutionContext)
      }
    )
  }

  private def recordOnLedger(commands: ApiCommands): Future[SubmissionResult] =
    for {
      handle <- ledgerBackend.beginSubmission
      res <- commandExecutor
        .execute(
          commands.submitter,
          commands,
          handle.lookupActiveContract(commands.submitter, _),
          handle.lookupContractKey(commands.submitter, _),
          commands.commands
        )
      submissionResult <- handleResult(res)
    } yield submissionResult

  private def handleResult(res: scala.Either[ErrorCause, TransactionSubmission]) =
    res match {
      case Right(ts) =>
        FutureConverters.toScala(
          writeService.submitTransaction(
            SubmitterInfo(
              Ref.Party.assertFromString(ts.submitter),
              ts.applicationId,
              ts.commandId,
              Timestamp.assertFromInstant(ts.maximumRecordTime)
            ),
            TransactionMeta(Timestamp.assertFromInstant(ts.ledgerEffectiveTime), ts.workflowId),
            ts.transaction
          ))
      case Left(err) => Future.failed(grpcError(toStatus(err)))
    }

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
