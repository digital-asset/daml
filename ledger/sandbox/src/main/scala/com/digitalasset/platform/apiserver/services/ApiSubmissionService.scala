// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.{Duration, Instant}
import java.util.UUID

import akka.stream.Materializer
import com.codahale.metrics.{Meter, MetricRegistry}
import com.daml.api.util.TimeProvider
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.domain.{LedgerId, Commands => ApiCommands}
import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.participant.state.index.v2.{
  CommandDeduplicationDuplicate,
  CommandDeduplicationNew,
  ContractStore,
  IndexPartyManagementService,
  IndexSubmissionService
}
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.SubmissionResult.{
  Acknowledged,
  InternalError,
  NotSupported,
  Overloaded
}
import com.daml.ledger.participant.state.v1.{SeedService, SubmissionResult, TimeModel, WriteService}
import com.daml.lf.crypto
import com.daml.lf.data.Ref.Party
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.transaction.BlindingInfo
import com.daml.lf.transaction.Transaction.Transaction
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.execution.{CommandExecutionResult, CommandExecutor}
import com.daml.platform.server.api.services.domain.CommandSubmissionService
import com.daml.platform.server.api.services.grpc.GrpcCommandSubmissionService
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.ErrorCause
import com.daml.timer.Delayed
import io.grpc.Status

import scala.collection.breakOut
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object ApiSubmissionService {

  type RecordUpdate = Either[LfError, (Transaction, BlindingInfo)]

  def create(
      ledgerId: LedgerId,
      contractStore: ContractStore,
      writeService: WriteService,
      submissionService: IndexSubmissionService,
      partyManagementService: IndexPartyManagementService,
      timeModel: TimeModel,
      timeProvider: TimeProvider,
      timeProviderType: TimeProviderType,
      seedService: Option[SeedService],
      commandExecutor: CommandExecutor,
      configuration: ApiSubmissionService.Configuration,
      metrics: MetricRegistry,
  )(
      implicit ec: ExecutionContext,
      mat: Materializer,
      logCtx: LoggingContext
  ): GrpcCommandSubmissionService with GrpcApiService =
    new GrpcCommandSubmissionService(
      service = new ApiSubmissionService(
        contractStore,
        writeService,
        submissionService,
        partyManagementService,
        timeModel,
        timeProvider,
        timeProviderType,
        seedService,
        commandExecutor,
        configuration,
        metrics,
      ),
      ledgerId = ledgerId,
      currentLedgerTime = () => timeProvider.getCurrentTime,
      currentUtcTime = () => Instant.now,
      maxDeduplicationTime = () => configuration.maxDeduplicationTime,
      metricRegistry = metrics,
    )

  object RecordUpdate {
    def apply(views: Either[LfError, (Transaction, BlindingInfo)]): RecordUpdate = views
  }

  final case class Configuration(
      // TODO(RA): this should be updated dynamically from the ledger configuration
      maxDeduplicationTime: Duration,
      implicitPartyAllocation: Boolean,
  )

}

final class ApiSubmissionService private (
    contractStore: ContractStore,
    writeService: WriteService,
    submissionService: IndexSubmissionService,
    partyManagementService: IndexPartyManagementService,
    timeModel: TimeModel,
    timeProvider: TimeProvider,
    timeProviderType: TimeProviderType,
    seedService: Option[SeedService],
    commandExecutor: CommandExecutor,
    configuration: ApiSubmissionService.Configuration,
    metrics: MetricRegistry,
)(implicit ec: ExecutionContext, mat: Materializer, logCtx: LoggingContext)
    extends CommandSubmissionService
    with ErrorFactories
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  private object Metrics {

    import com.daml.platform.server.api.services.grpc.GrpcCommandSubmissionService.MetricPrefix

    val failedInterpretationsMeter: Meter =
      metrics.meter(MetricPrefix :+ "failed_command_interpretations")
    val deduplicatedCommandsMeter: Meter =
      metrics.meter(MetricPrefix :+ "deduplicated_commands")
    val delayedSubmissionsMeter: Meter =
      metrics.meter(MetricPrefix :+ "delayed_submissions")
    val validSubmissionsMeter: Meter =
      metrics.meter(MetricPrefix :+ "valid_submissions")
  }

  private def deduplicateAndRecordOnLedger(seed: Option[crypto.Hash], commands: ApiCommands)(
      implicit logCtx: LoggingContext): Future[Unit] = {
    val submittedAt = commands.submittedAt
    val deduplicateUntil = commands.deduplicateUntil

    submissionService
      .deduplicateCommand(commands.commandId, commands.submitter, submittedAt, deduplicateUntil)
      .flatMap {
        case CommandDeduplicationNew =>
          recordOnLedger(seed, commands)
            .transform(mapSubmissionResult)
            .recoverWith {
              case error =>
                submissionService
                  .stopDeduplicatingCommand(commands.commandId, commands.submitter)
                  .transform(_ => Failure(error))
            }
        case CommandDeduplicationDuplicate(until) =>
          Metrics.deduplicatedCommandsMeter.mark()
          val reason =
            s"A command with the same command ID ${commands.commandId} and submitter ${commands.submitter} was submitted before. Deduplication window until $until"
          logger.debug(reason)
          Future.failed(Status.ALREADY_EXISTS.augmentDescription(reason).asRuntimeException)
      }
  }

  override def submit(request: SubmitRequest): Future[Unit] =
    withEnrichedLoggingContext(
      logging.commandId(request.commands.commandId),
      logging.party(request.commands.submitter)) { implicit logCtx =>
      val commands = request.commands

      logger.trace(s"Received composite commands: $commands")
      logger.debug(s"Received composite command let ${commands.commands.ledgerEffectiveTime}.")
      deduplicateAndRecordOnLedger(seedService.map(_.nextSeed()), commands)
        .andThen(logger.logErrorsOnCall[Unit])(DirectExecutionContext)
    }

  private def mapSubmissionResult(result: Try[SubmissionResult])(
      implicit logCtx: LoggingContext): Try[Unit] = result match {
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
      logger.info(s"Submission of command rejected: ${error.getMessage}")
      Failure(error)
  }

  private def recordOnLedger(
      submissionSeed: Option[crypto.Hash],
      commands: ApiCommands,
  )(implicit logCtx: LoggingContext): Future[SubmissionResult] =
    for {
      res <- commandExecutor.execute(commands, submissionSeed)
      transactionInfo <- res.fold(error => {
        Metrics.failedInterpretationsMeter.mark()
        Future.failed(grpcError(toStatus(error)))
      }, Future.successful)
      partyAllocationResults <- allocateMissingInformees(transactionInfo.transaction)
      submissionResult <- submitTransaction(transactionInfo, partyAllocationResults)
    } yield submissionResult

  private def allocateMissingInformees(
      transaction: Transaction,
  ): Future[Seq[SubmissionResult]] =
    if (configuration.implicitPartyAllocation) {
      val parties: Set[Party] = transaction.nodes.values.flatMap(_.informeesOfNode)(breakOut)
      partyManagementService.getParties(parties.toSeq).flatMap { partyDetails =>
        val missingParties = parties -- partyDetails.map(_.party)
        if (missingParties.nonEmpty) {
          logger.info(s"Implicitly allocating the parties: ${missingParties.mkString(", ")}")
          Future.sequence(
            missingParties.toSeq
              .map(name =>
                writeService.allocateParty(
                  hint = Some(name),
                  displayName = Some(name),
                  // TODO: Just like the ApiPartyManagementService, this should do proper validation.
                  submissionId = v1.SubmissionId.assertFromString(UUID.randomUUID().toString),
              ))
              .map(_.toScala))
        } else {
          Future.successful(Seq.empty)
        }
      }
    } else {
      Future.successful(Seq.empty)
    }

  private def submitTransaction(
      transactionInfo: CommandExecutionResult,
      partyAllocationResults: Seq[SubmissionResult],
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
              .minus(timeModel.avgTransactionLatency)
            val submissionDelay = Duration.between(timeProvider.getCurrentTime, submitAt)
            if (submissionDelay.isNegative)
              submitTransaction(transactionInfo)
            else {
              Metrics.delayedSubmissionsMeter.mark()
              val scalaDelay = scala.concurrent.duration.Duration.fromNanos(submissionDelay.toNanos)
              Delayed.Future.by(scalaDelay)(submitTransaction(transactionInfo))
            }
          case TimeProviderType.Static =>
            // In static time mode, record time is always equal to ledger time
            submitTransaction(transactionInfo)
        }
    }

  private def submitTransaction(
      result: CommandExecutionResult,
  ): Future[SubmissionResult] = {
    Metrics.validSubmissionsMeter.mark()
    writeService
      .submitTransaction(result.submitterInfo, result.transactionMeta, result.transaction)
      .toScala
  }

  private def toStatus(errorCause: ErrorCause) =
    errorCause match {
      case e: ErrorCause.DamlLf =>
        Status.INVALID_ARGUMENT.withDescription(e.explain)
      case e: ErrorCause.LedgerTime =>
        Status.ABORTED.withDescription(e.explain)
    }

  override def close(): Unit = ()

}
