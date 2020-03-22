// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver.services

import java.time.{Duration, Instant}
import java.util.UUID

import akka.stream.Materializer
import com.codahale.metrics.{Meter, MetricRegistry, Timer}
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
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.engine.{Error => LfError}
import com.digitalasset.daml.lf.transaction.Transaction.Transaction
import com.digitalasset.daml.lf.transaction.{BlindingInfo, Transaction}
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.domain.{LedgerId, Commands => ApiCommands}
import com.digitalasset.ledger.api.messages.command.submission.SubmitRequest
import com.digitalasset.logging.LoggingContext.withEnrichedLoggingContext
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.apiserver.{
  CommandExecutionResult,
  CommandExecutor,
  LedgerTimeHelper
}
import com.digitalasset.platform.metrics.timedFuture
import com.digitalasset.platform.server.api.services.domain.CommandSubmissionService
import com.digitalasset.platform.server.api.services.grpc.GrpcCommandSubmissionService
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.store.ErrorCause
import io.grpc.Status
import scalaz.syntax.tag._

import scala.collection.breakOut
import scala.compat.java8.FutureConverters
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
      new ApiSubmissionService(
        contractStore,
        writeService,
        submissionService,
        partyManagementService,
        timeModel,
        timeProvider,
        seedService,
        commandExecutor,
        configuration,
        metrics,
      ),
      ledgerId,
      () => Instant.now(),
      () => configuration.maxDeduplicationTime,
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
    seedService: Option[SeedService],
    commandExecutor: CommandExecutor,
    configuration: ApiSubmissionService.Configuration,
    metrics: MetricRegistry,
)(implicit ec: ExecutionContext, mat: Materializer, logCtx: LoggingContext)
    extends CommandSubmissionService
    with ErrorFactories
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  private[this] val ledgerTimeHelper = LedgerTimeHelper(contractStore, commandExecutor, 3)

  private object Metrics {
    val failedInterpretationsMeter: Meter =
      metrics.meter("daml.lapi.command_submission_service.failed_command_interpretations")

    val deduplicatedCommandsMeter: Meter =
      metrics.meter("daml.lapi.command_submission_service.deduplicated_commands")

    val submittedTransactionsTimer: Timer =
      metrics.timer("daml.lapi.command_submission_service.submitted_transactions")
  }

  private def deduplicateAndRecordOnLedger(seed: Option[crypto.Hash], commands: ApiCommands)(
      implicit logCtx: LoggingContext): Future[Unit] = {
    val deduplicationKey = commands.submitter + "%" + commands.commandId.unwrap
    val submittedAt = commands.submittedAt
    val deduplicateUntil = commands.deduplicateUntil

    submissionService.deduplicateCommand(deduplicationKey, submittedAt, deduplicateUntil).flatMap {
      case CommandDeduplicationNew =>
        recordOnLedger(seed, commands)
          .transform(mapSubmissionResult)
      case CommandDeduplicationDuplicate(_) =>
        Metrics.deduplicatedCommandsMeter.mark()
        val reason = s"A command with the same command ID and submitter was submitted before."
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
      logger.debug(s"Received composite command let ${commands.ledgerEffectiveTime}.")
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
      res <- ledgerTimeHelper.execute(commands, submissionSeed)
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
        transactionInfo match {
          case CommandExecutionResult(submitterInfo, transactionMeta, transaction, _) =>
            timedFuture(
              Metrics.submittedTransactionsTimer,
              FutureConverters.toScala(
                writeService.submitTransaction(submitterInfo, transactionMeta, transaction)))
        }
    }

  private def toStatus(errorCause: ErrorCause) =
    errorCause match {
      case e @ ErrorCause.DamlLf(_) =>
        Status.INVALID_ARGUMENT.withDescription(e.explain)
      case e @ ErrorCause.Sequencer(errors) =>
        val base = if (errors.exists(_.isFinal)) Status.INVALID_ARGUMENT else Status.ABORTED
        base.withDescription(e.explain)
      case e @ ErrorCause.LedgerTime(_) =>
        Status.ABORTED.withDescription(e.explain)
    }

  override def close(): Unit = ()

}
