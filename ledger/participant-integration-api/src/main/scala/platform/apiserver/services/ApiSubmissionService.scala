// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.{Duration, Instant}
import java.util.UUID

import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain.{LedgerId, Commands => ApiCommands}
import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.participant.state.index.v2._
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.SubmissionResult.{
  Acknowledged,
  InternalError,
  NotSupported,
  Overloaded,
}
import com.daml.ledger.participant.state.v1.{
  Configuration,
  SeedService,
  SubmissionResult,
  WriteService,
}
import com.daml.lf.crypto
import com.daml.lf.data.Ref.Party
import com.daml.lf.engine.{ContractNotFound, ReplayMismatch}
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.execution.{CommandExecutionResult, CommandExecutor}
import com.daml.platform.server.api.services.domain.CommandSubmissionService
import com.daml.platform.server.api.services.grpc.GrpcCommandSubmissionService
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.ErrorCause
import com.daml.timer.Delayed
import io.grpc.Status

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

private[apiserver] object ApiSubmissionService {

  def create(
      ledgerId: LedgerId,
      writeService: WriteService,
      submissionService: IndexSubmissionService,
      partyManagementService: IndexPartyManagementService,
      timeProvider: TimeProvider,
      timeProviderType: TimeProviderType,
      ledgerConfigProvider: LedgerConfigProvider,
      seedService: SeedService,
      commandExecutor: CommandExecutor,
      configuration: ApiSubmissionService.Configuration,
      metrics: Metrics,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): GrpcCommandSubmissionService with GrpcApiService =
    new GrpcCommandSubmissionService(
      service = new ApiSubmissionService(
        writeService,
        submissionService,
        partyManagementService,
        timeProvider,
        timeProviderType,
        ledgerConfigProvider,
        seedService,
        commandExecutor,
        configuration,
        metrics,
      ),
      ledgerId = ledgerId,
      currentLedgerTime = () => timeProvider.getCurrentTime,
      currentUtcTime = () => Instant.now,
      maxDeduplicationTime = () =>
        ledgerConfigProvider.latestConfiguration.map(_.maxDeduplicationTime),
      metrics = metrics,
    )

  final case class Configuration(
      implicitPartyAllocation: Boolean
  )

}

private[apiserver] final class ApiSubmissionService private[services] (
    writeService: WriteService,
    submissionService: IndexSubmissionService,
    partyManagementService: IndexPartyManagementService,
    timeProvider: TimeProvider,
    timeProviderType: TimeProviderType,
    ledgerConfigProvider: LedgerConfigProvider,
    seedService: SeedService,
    commandExecutor: CommandExecutor,
    configuration: ApiSubmissionService.Configuration,
    metrics: Metrics,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends CommandSubmissionService
    with ErrorFactories
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val DuplicateCommand = Status.ALREADY_EXISTS.augmentDescription("Duplicate command")

  override def submit(request: SubmitRequest): Future[Unit] =
    withEnrichedLoggingContext(logging.commands(request.commands)) { implicit loggingContext =>
      logger.info("Submitting transaction")
      logger.trace(s"Commands: ${request.commands.commands.commands}")
      ledgerConfigProvider.latestConfiguration
        .map(deduplicateAndRecordOnLedger(seedService.nextSeed(), request.commands, _))
        .getOrElse(Future.failed(ErrorFactories.missingLedgerConfig()))
        .andThen(logger.logErrorsOnCall[Unit])
    }

  private def deduplicateAndRecordOnLedger(
      seed: crypto.Hash,
      commands: ApiCommands,
      ledgerConfig: Configuration,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    submissionService
      .deduplicateCommand(
        commands.commandId,
        commands.actAs.toList,
        commands.submittedAt,
        commands.deduplicateUntil,
      )
      .flatMap {
        case CommandDeduplicationNew =>
          evaluateAndSubmit(seed, commands, ledgerConfig)
            .transform(handleSubmissionResult)
            .recoverWith { case NonFatal(originalCause) =>
              submissionService
                .stopDeduplicatingCommand(commands.commandId, commands.actAs.toList)
                .transform(_ => Failure(originalCause))
            }
        case _: CommandDeduplicationDuplicate =>
          metrics.daml.commands.deduplicatedCommands.mark()
          logger.debug(DuplicateCommand.getDescription)
          Future.failed(DuplicateCommand.asRuntimeException)
      }

  private def handleSubmissionResult(result: Try[SubmissionResult])(implicit
      loggingContext: LoggingContext
  ): Try[Unit] = result match {
    case Success(Acknowledged) =>
      logger.debug("Success")
      Success(())

    case Success(Overloaded) =>
      logger.info("Back-pressure")
      Failure(Status.RESOURCE_EXHAUSTED.asRuntimeException)

    case Success(NotSupported) =>
      logger.warn("Not supported")
      Failure(Status.INVALID_ARGUMENT.asRuntimeException)

    case Success(InternalError(reason)) =>
      logger.error(s"Internal error: $reason")
      Failure(Status.INTERNAL.augmentDescription(reason).asRuntimeException)

    case Failure(error) =>
      logger.info(s"Rejected: ${error.getMessage}")
      Failure(error)
  }

  private def handleCommandExecutionResult(
      result: Either[ErrorCause, CommandExecutionResult]
  ): Future[CommandExecutionResult] =
    result.fold(
      error => {
        metrics.daml.commands.failedCommandInterpretations.mark()
        Future.failed(grpcError(toStatus(error)))
      },
      Future.successful,
    )

  private def evaluateAndSubmit(
      submissionSeed: crypto.Hash,
      commands: ApiCommands,
      ledgerConfig: Configuration,
  )(implicit loggingContext: LoggingContext): Future[SubmissionResult] =
    for {
      result <- commandExecutor.execute(commands, submissionSeed)
      transactionInfo <- handleCommandExecutionResult(result)
      partyAllocationResults <- allocateMissingInformees(transactionInfo.transaction)
      submissionResult <- submitTransaction(transactionInfo, partyAllocationResults, ledgerConfig)
    } yield submissionResult

  // Takes the whole transaction to ensure to traverse it only if necessary
  private[services] def allocateMissingInformees(
      transaction: SubmittedTransaction
  )(implicit loggingContext: LoggingContext): Future[Seq[SubmissionResult]] =
    if (configuration.implicitPartyAllocation) {
      val partiesInTransaction =
        transaction.nodes.valuesIterator.flatMap(_.informeesOfNode).toSeq.distinct
      for {
        fetchedParties <- partyManagementService.getParties(partiesInTransaction)
        knownParties = fetchedParties.iterator.map(_.party).toSet
        missingParties = partiesInTransaction.filterNot(knownParties)
        submissionResults <- Future.sequence(missingParties.map(allocateParty))
      } yield submissionResults
    } else Future.successful(Seq.empty)

  private def allocateParty(name: Party) = {
    val submissionId = v1.SubmissionId.assertFromString(UUID.randomUUID().toString)
    withEnrichedLoggingContext(logging.party(name), logging.submissionId(submissionId)) {
      implicit loggingContext =>
        logger.info("Implicit party allocation")
        writeService
          .allocateParty(
            hint = Some(name),
            displayName = Some(name),
            submissionId = submissionId,
          )
    }
  }.toScala

  private def submitTransaction(
      transactionInfo: CommandExecutionResult,
      partyAllocationResults: Seq[SubmissionResult],
      ledgerConfig: Configuration,
  ): Future[SubmissionResult] =
    partyAllocationResults.find(_ != SubmissionResult.Acknowledged) match {
      case Some(result) =>
        Future.successful(result)
      case None =>
        timeProviderType match {
          case TimeProviderType.WallClock =>
            // Submit transactions such that they arrive at the ledger sequencer exactly when record time equals ledger time.
            // If the ledger time of the transaction is far in the future (farther than the expected latency),
            // the submission to the WriteService is delayed.
            val submitAt = transactionInfo.transactionMeta.ledgerEffectiveTime.toInstant
              .minus(ledgerConfig.timeModel.avgTransactionLatency)
            val submissionDelay = Duration.between(timeProvider.getCurrentTime, submitAt)
            if (submissionDelay.isNegative)
              submitTransaction(transactionInfo)
            else {
              metrics.daml.commands.delayedSubmissions.mark()
              val scalaDelay = scala.concurrent.duration.Duration.fromNanos(submissionDelay.toNanos)
              Delayed.Future.by(scalaDelay)(submitTransaction(transactionInfo))
            }
          case TimeProviderType.Static =>
            // In static time mode, record time is always equal to ledger time
            submitTransaction(transactionInfo)
        }
    }

  private def submitTransaction(
      result: CommandExecutionResult
  ): Future[SubmissionResult] = {
    metrics.daml.commands.validSubmissions.mark()
    writeService
      .submitTransaction(
        result.submitterInfo,
        result.transactionMeta,
        result.transaction,
        result.interpretationTimeNanos,
      )
      .toScala
  }

  private def toStatus(errorCause: ErrorCause) =
    errorCause match {
      case cause @ ErrorCause.DamlLf(error) =>
        error match {
          case ContractNotFound(_) | ReplayMismatch(_) =>
            Status.ABORTED.withDescription(cause.explain)
          case _ => Status.INVALID_ARGUMENT.withDescription(cause.explain)
        }
      case cause: ErrorCause.LedgerTime =>
        Status.ABORTED.withDescription(cause.explain)
    }

  override def close(): Unit = ()

}
