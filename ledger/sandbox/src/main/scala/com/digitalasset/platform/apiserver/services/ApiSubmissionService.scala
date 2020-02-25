// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver.services

import java.time.Instant

import akka.stream.Materializer
import com.codahale.metrics.{Meter, MetricRegistry, Timer}
import com.daml.ledger.participant.state.index.v2.{
  CommandDeduplicationDuplicate,
  CommandDeduplicationDuplicateWithResult,
  CommandDeduplicationNew,
  ContractStore,
  IndexSubmissionService
}
import com.daml.ledger.participant.state.v1.SubmissionResult.{
  Acknowledged,
  InternalError,
  NotSupported,
  Overloaded
}
import com.daml.ledger.participant.state.v1.{
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
import com.digitalasset.logging.LoggingContext.withEnrichedLoggingContext
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.apiserver.CommandExecutor
import com.digitalasset.platform.metrics.timedFuture
import com.digitalasset.platform.server.api.services.domain.CommandSubmissionService
import com.digitalasset.platform.server.api.services.grpc.GrpcCommandSubmissionService
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.server.services.command.time.TimeModelValidator
import com.digitalasset.platform.store.ErrorCause
import io.grpc.{Status, StatusRuntimeException}
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
      submissionService: IndexSubmissionService,
      timeModel: TimeModel,
      timeProvider: TimeProvider,
      commandExecutor: CommandExecutor,
      metrics: MetricRegistry)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      logCtx: LoggingContext): GrpcCommandSubmissionService with GrpcApiService =
    new GrpcCommandSubmissionService(
      new ApiSubmissionService(
        contractStore,
        writeService,
        submissionService,
        timeModel,
        timeProvider,
        commandExecutor,
        metrics),
      ledgerId
    )

  object RecordUpdate {
    def apply(views: Either[LfError, (Transaction, BlindingInfo)]): RecordUpdate = views
  }

}

final class ApiSubmissionService private (
    contractStore: ContractStore,
    writeService: WriteService,
    submissionService: IndexSubmissionService,
    timeModel: TimeModel,
    timeProvider: TimeProvider,
    commandExecutor: CommandExecutor,
    metrics: MetricRegistry)(
    implicit ec: ExecutionContext,
    mat: Materializer,
    logCtx: LoggingContext)
    extends CommandSubmissionService
    with ErrorFactories
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  // FIXME(JM): We need to query the current configuration every time we want to validate
  // a command. Will be addressed in follow-up PR.
  private val validator = TimeModelValidator(timeModel)

  private val maxTtl: Long = 60 * 60

  private object Metrics {
    val failedInterpretationsMeter: Meter =
      metrics.meter("daml.lapi.command_submission_service.failed_command_interpretations")

    val deduplicatedCommandsMeter: Meter =
      metrics.meter("daml.lapi.command_submission_service.deduplicated_commands")

    val submittedTransactionsTimer: Timer =
      metrics.timer("daml.lapi.command_submission_service.submitted_transactions")
  }

  private def deduplicateAndRecordOnLedger(commands: ApiCommands)(
      implicit logCtx: LoggingContext): Future[Unit] = {
    val deduplicationKey = commands.submitter + "%" + commands.commandId.unwrap
    val submittedAt = Instant.now
    val ttl = submittedAt.plusSeconds(commands.ttl.getOrElse(maxTtl))

    submissionService.deduplicateCommand(deduplicationKey, submittedAt, ttl).flatMap {
      case CommandDeduplicationNew =>
        recordOnLedger(commands)
          .andThen {
            case Success(_) =>
              submissionService.updateCommandResult(deduplicationKey, submittedAt, Right(()))
            case Failure(error: StatusRuntimeException) =>
              // TODO: store correct status instead of always returning INTERNAL
              submissionService
                .updateCommandResult(deduplicationKey, submittedAt, Left(error.getMessage))
            case Failure(error) =>
              submissionService
                .updateCommandResult(deduplicationKey, submittedAt, Left(error.getMessage))
          }
      case CommandDeduplicationDuplicate(firstSubmittedAt) =>
        Metrics.deduplicatedCommandsMeter.mark()
        val reason =
          s"Duplicate command submission. This command was submitted before at $firstSubmittedAt. The result of the submission is unknown."
        logger.debug(reason)
        Future.failed(Status.ALREADY_EXISTS.augmentDescription(reason).asRuntimeException)
      case CommandDeduplicationDuplicateWithResult(Left(error)) =>
        // TODO: store correct status instead of always returning INTERNAL
        Metrics.deduplicatedCommandsMeter.mark()
        Future.failed(Status.INTERNAL.augmentDescription(error).asRuntimeException)
      case CommandDeduplicationDuplicateWithResult(Right(())) =>
        Metrics.deduplicatedCommandsMeter.mark()
        Future.successful(())
    }
  }

  override def submit(request: SubmitRequest): Future[Unit] =
    withEnrichedLoggingContext(
      logging.commandId(request.commands.commandId),
      logging.party(request.commands.submitter)) { implicit logCtx =>
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

      validation
        .fold(
          Future.failed,
          _ => {
            logger.trace(s"Received composite commands: $commands")
            logger.debug(s"Received composite command let ${commands.ledgerEffectiveTime}.")
            deduplicateAndRecordOnLedger(commands)
          }
        )
        .andThen(logger.logErrorsOnCall[Unit])(DirectExecutionContext)
    }

  private def recordOnLedger(commands: ApiCommands)(implicit logCtx: LoggingContext): Future[Unit] =
    (for {
      res <- commandExecutor
        .execute(
          commands.submitter,
          commands,
          contractStore.lookupActiveContract(commands.submitter, _),
          contractStore.lookupContractKey(commands.submitter, _),
          commands.commands
        )
      submissionResult <- handleResult(res)
    } yield submissionResult).transform {
      case Success(Acknowledged) =>
        logger.debug("Submission of command succeeded")
        Success(())

      case Success(Overloaded) =>
        logger.info("Submission has failed due to backpressure")
        Failure(Status.RESOURCE_EXHAUSTED.asRuntimeException)

      case Success(NotSupported) =>
        logger.warn("Submission of command was not supported")
        Failure(Status.INVALID_ARGUMENT.asRuntimeException)

      case Success(InternalError(reason)) =>
        logger.error(s"Submission of command failed due to an internal error, reason=$reason")
        Failure(Status.INTERNAL.augmentDescription(reason).asRuntimeException)

      case Failure(error) =>
        logger.error("Submission of command has failed.", error)
        Failure(error)

    }(DirectExecutionContext)

  private def handleResult(
      res: scala.Either[ErrorCause, (SubmitterInfo, TransactionMeta, Transaction.Transaction)]
  ) =
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
      case e @ ErrorCause.DamlLf(_) =>
        Status.INVALID_ARGUMENT.withDescription(e.explain)
      case e @ ErrorCause.Sequencer(errors) =>
        val base = if (errors.exists(_.isFinal)) Status.INVALID_ARGUMENT else Status.ABORTED
        base.withDescription(e.explain)
    }
  }

  override def close(): Unit = ()

}
