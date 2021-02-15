// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.util.UUID

import com.daml.ledger.api.v1.admin.participant_pruning_service.{
  ParticipantPruningServiceGrpc,
  PruneRequest,
  PruneResponse,
}
import com.daml.ledger.participant.state.index.v2.{IndexParticipantPruningService, LedgerEndService}
import com.daml.ledger.participant.state.v1.{
  Offset,
  PruningResult,
  SubmissionId,
  WriteParticipantPruningService,
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.apiserver.services.logging
import com.daml.platform.ApiOffset
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}

final class ApiParticipantPruningService private (
    readBackend: IndexParticipantPruningService with LedgerEndService,
    writeBackend: WriteParticipantPruningService,
)(implicit executionContext: ExecutionContext, logCtx: LoggingContext)
    extends ParticipantPruningServiceGrpc.ParticipantPruningService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def bindService(): ServerServiceDefinition =
    ParticipantPruningServiceGrpc.bindService(this, executionContext)

  override def prune(request: PruneRequest): Future[PruneResponse] = {
    val submissionIdOrErr = SubmissionId
      .fromString(
        if (request.submissionId.nonEmpty) request.submissionId else UUID.randomUUID().toString
      )
      .left
      .map(err => ErrorFactories.invalidArgument(s"submission_id $err"))

    submissionIdOrErr.fold(
      Future.failed,
      submissionId =>
        LoggingContext.withEnrichedLoggingContext(logging.submissionId(submissionId)) {
          implicit logCtx =>
            logger.info(s"Pruning up to ${request.pruneUpTo}")
            (for {

              pruneUpTo <- validateRequest(request: PruneRequest)

              // If write service pruning succeeds but ledger api server index pruning fails, the user can bring the
              // systems back in sync by reissuing the prune request at the currently specified or later offset.
              _ <- pruneWriteService(pruneUpTo, submissionId)

              pruneResponse <- pruneLedgerApiServerIndex(pruneUpTo)

            } yield pruneResponse).andThen(logger.logErrorsOnCall[PruneResponse])
        },
    )
  }

  private def validateRequest(
      request: PruneRequest
  )(implicit logCtx: LoggingContext): Future[Offset] = {
    (for {
      pruneUpToString <- checkOffsetIsSpecified(request.pruneUpTo)
      pruneUpTo <- checkOffsetIsHexadecimal(pruneUpToString)
    } yield (pruneUpTo, pruneUpToString))
      .fold(Future.failed, o => checkOffsetIsBeforeLedgerEnd(o._1, o._2))
  }

  private def pruneWriteService(pruneUpTo: Offset, submissionId: SubmissionId)(implicit
      logCtx: LoggingContext
  ): Future[Unit] = {
    logger.info(
      s"About to prune participant ledger up to ${pruneUpTo.toApiString} inclusively starting with the write service"
    )
    FutureConverters
      .toScala(writeBackend.prune(pruneUpTo, submissionId))
      .flatMap {
        case PruningResult.NotPruned(status) => Future.failed(ErrorFactories.grpcError(status))
        case PruningResult.ParticipantPruned =>
          logger.info(s"Pruned participant ledger up to ${pruneUpTo.toApiString} inclusively.")
          Future.successful(())
      }
  }

  private def pruneLedgerApiServerIndex(
      pruneUpTo: Offset
  )(implicit logCtx: LoggingContext): Future[PruneResponse] = {
    logger.info(s"About to prune ledger api server index to ${pruneUpTo.toApiString} inclusively")
    readBackend
      .prune(pruneUpTo)
      .map { _ =>
        logger.info(s"Pruned ledger api server index up to ${pruneUpTo.toApiString} inclusively.")
        PruneResponse()
      }
  }

  private def checkOffsetIsSpecified(offset: String): Either[StatusRuntimeException, String] =
    Either.cond(
      !offset.isEmpty,
      offset,
      ErrorFactories.invalidArgument("prune_up_to not specified"),
    )

  private def checkOffsetIsHexadecimal(
      pruneUpToString: String
  ): Either[StatusRuntimeException, Offset] =
    ApiOffset
      .fromString(pruneUpToString)
      .toEither
      .left
      .map(t =>
        ErrorFactories.invalidArgument(
          s"prune_up_to needs to be a hexadecimal string and not ${pruneUpToString}: ${t.getMessage}"
        )
      )

  private def checkOffsetIsBeforeLedgerEnd(pruneUpToProto: Offset, pruneUpToString: String)(implicit
      logCtx: LoggingContext
  ): Future[Offset] =
    for {
      ledgerEnd <- readBackend.currentLedgerEnd()
      _ <-
        if (pruneUpToString < ledgerEnd.value) Future.successful(())
        else
          Future.failed(
            ErrorFactories.invalidArgument(
              s"prune_up_to needs to be before ledger end ${ledgerEnd.value}"
            )
          )
    } yield pruneUpToProto

  override def close(): Unit = ()

}

object ApiParticipantPruningService {
  def createApiService(
      readBackend: IndexParticipantPruningService with LedgerEndService,
      writeBackend: WriteParticipantPruningService,
  )(implicit
      executionContext: ExecutionContext,
      logCtx: LoggingContext,
  ): ParticipantPruningServiceGrpc.ParticipantPruningService with GrpcApiService =
    new ApiParticipantPruningService(readBackend, writeBackend)

}
