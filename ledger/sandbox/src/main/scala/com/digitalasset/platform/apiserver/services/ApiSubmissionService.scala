// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver.services

import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.participant.state.v1.SubmissionResult.{
  Acknowledged,
  InternalError,
  NotSupported,
  Overloaded
}
import com.daml.ledger.participant.state.v1.{
  SubmissionResult,
  SubmitterInfo,
  TimeModel,
  TransactionMeta,
  WriteService
}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.engine.{Error => LfError}
import com.digitalasset.daml.lf.transaction.Transaction.Transaction
import com.digitalasset.daml.lf.transaction.{BlindingInfo, Transaction}
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.domain.{LedgerId, Commands => ApiCommands}
import com.digitalasset.ledger.api.messages.command.submission.SubmitRequest
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceLogging
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.sandbox.metrics.timedFuture
import com.digitalasset.platform.sandbox.stores.ledger.{CommandExecutor, ErrorCause}
import com.digitalasset.platform.server.api.services.domain.CommandSubmissionService
import com.digitalasset.platform.server.api.services.grpc.GrpcCommandSubmissionService
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.server.services.command.time.TimeModelValidator
import io.grpc.Status
import scalaz.syntax.tag._

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ApiSubmissionService {

  type RecordUpdate = Either[LfError, (Transaction, BlindingInfo)]

  def create(
      ledgerId: LedgerId,
      contractStore: ContractStore,
      writeService: WriteService,
      timeModel: TimeModel,
      timeProvider: TimeProvider,
      commandExecutor: CommandExecutor,
      loggerFactory: NamedLoggerFactory,
      metrics: MetricRegistry)(implicit ec: ExecutionContext, mat: Materializer)
    : GrpcCommandSubmissionService with GrpcApiService with CommandSubmissionServiceLogging =
    new GrpcCommandSubmissionService(
      new ApiSubmissionService(
        contractStore,
        writeService,
        timeModel,
        timeProvider,
        commandExecutor,
        loggerFactory,
        metrics),
      ledgerId
    ) with CommandSubmissionServiceLogging

  object RecordUpdate {
    def apply(views: Either[LfError, (Transaction, BlindingInfo)]): RecordUpdate = views
  }

}

class ApiSubmissionService private (
    contractStore: ContractStore,
    writeService: WriteService,
    timeModel: TimeModel,
    timeProvider: TimeProvider,
    commandExecutor: CommandExecutor,
    loggerFactory: NamedLoggerFactory,
    metrics: MetricRegistry)(implicit ec: ExecutionContext, mat: Materializer)
    extends CommandSubmissionService
    with ErrorFactories
    with AutoCloseable {

  private val logger = loggerFactory.getLogger(this.getClass)

  // FIXME(JM): We need to query the current configuration every time we want to validate
  // a command. Will be addressed in follow-up PR.
  private val validator = TimeModelValidator(timeModel)

  private object Metrics {
    val failedInterpretationsMeter =
      metrics.meter("CommandSubmission.failedCommandInterpretations")

    val submittedTransactionsTimer =
      metrics.timer("CommandSubmission.submittedTransactions")
  }

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

          case Success(NotSupported) =>
            logger.debug(s"Submission of command {} was not supported", commands.commandId.unwrap)
            Failure(Status.INVALID_ARGUMENT.asRuntimeException)

          case Success(InternalError(reason)) =>
            logger.debug(
              s"Submission of command ${commands.commandId.unwrap} failed due to an internal error, reason=$reason ")
            Failure(Status.INTERNAL.augmentDescription(reason).asRuntimeException)

          case Failure(error) =>
            logger.warn(s"Submission of command ${commands.commandId.unwrap} has failed.", error)
            Failure(error)

        }(DirectExecutionContext)
      }
    )
  }

  private def recordOnLedger(commands: ApiCommands): Future[SubmissionResult] =
    for {
      res <- commandExecutor
        .execute(
          commands.submitter,
          commands,
          contractStore.lookupActiveContract(commands.submitter, _),
          contractStore.lookupContractKey(commands.submitter, _),
          commands.commands
        )
      submissionResult <- handleResult(res)
    } yield submissionResult

  private def handleResult(
      res: scala.Either[ErrorCause, (SubmitterInfo, TransactionMeta, Transaction.Transaction)]) =
    res match {
      case Right((submitterInfo, transactionMeta, transaction)) =>
        timedFuture(
          Metrics.submittedTransactionsTimer,
          FutureConverters.toScala(
            writeService.submitTransaction(
              submitterInfo,
              transactionMeta,
              transaction
            )))
      case Left(err) =>
        Metrics.failedInterpretationsMeter.mark()
        Future.failed(grpcError(toStatus(err)))
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
