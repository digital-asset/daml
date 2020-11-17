// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.util.UUID

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.v1.admin.participant_pruning_service.{
  ParticipantPruningServiceGrpc,
  PruneRequest
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.participant.state.index.v2.{IndexParticipantPruningService, LedgerEndService}
import com.daml.ledger.participant.state.v1.{
  NotPruned,
  Offset,
  ParticipantPruned,
  SubmissionId,
  WriteParticipantPruningService
}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.ErrorFactories
import com.google.protobuf.empty.Empty
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}

final class ApiParticipantPruningService private (
    readBackend: IndexParticipantPruningService with LedgerEndService,
    writeBackend: WriteParticipantPruningService)(
    implicit grpcExecutionContext: ExecutionContext,
    logCtx: LoggingContext)
    extends ParticipantPruningServiceGrpc.ParticipantPruningService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def bindService(): ServerServiceDefinition =
    ParticipantPruningServiceGrpc.bindService(this, DirectExecutionContext)

  override def prune(request: PruneRequest): Future[Empty] = {
    val submissionId =
      if (request.submissionId.isEmpty)
        SubmissionId.assertFromString(UUID.randomUUID().toString)
      else
        SubmissionId.assertFromString(request.submissionId)

    LoggingContext.withEnrichedLoggingContext("submissionId" -> submissionId) { implicit logCtx =>
      val pruneUpToOffset: Either[StatusRuntimeException, (Offset, String)] = for {
        pruneUpToProto <- request.pruneUpTo.toRight(
          ErrorFactories.invalidArgument("prune_up_to not specified"))

        pruneUpToString <- pruneUpToProto.value match {
          case LedgerOffset.Value.Absolute(offset) => Right(offset)
          case LedgerOffset.Value.Boundary(boundary) =>
            Left(
              ErrorFactories.invalidArgument(
                s"prune_up_to needs to be absolute and not a boundary ${boundary.toString}"))
          case LedgerOffset.Value.Empty =>
            Left(
              ErrorFactories.invalidArgument(
                s"prune_up_to needs to be absolute and not empty ${pruneUpToProto.value}"))
        }

        pruneUpTo <- Ref.HexString
          .fromString(pruneUpToString)
          .left
          .map(err =>
            ErrorFactories.invalidArgument(
              s"prune_up_to needs to be a hexadecimal string and not ${pruneUpToString}: ${err}"))
          .map(Offset.fromHexString)

      } yield (pruneUpTo, pruneUpToString)

      pruneUpToOffset.fold(
        Future.failed, {
          case (pruneUpTo, pruneUpToString) =>
            (for {
              ledgerEnd <- readBackend.currentLedgerEnd()
              _ <- if (pruneUpToString < ledgerEnd.value) Future.successful(())
              else
                Future.failed(
                  ErrorFactories.invalidArgument(
                    s"prune_up_to needs to be before ledger end ${ledgerEnd.value}"))

              _ <- FutureConverters.toScala(writeBackend.prune(pruneUpTo, submissionId)).flatMap {
                case NotPruned(status) => Future.failed(ErrorFactories.fromGrpcStatus(status))
                case ParticipantPruned =>
                  logger.info(s"Pruned participant ledger up to ${pruneUpTo.toString} inclusively.")
                  Future.successful(())
              }

              pruneResponse <- readBackend
                .prune(pruneUpTo)
                .map { _ =>
                  logger.info(s"Pruned ledger api server index up to ${pruneUpTo} inclusively.")
                  Empty()
                }

            } yield pruneResponse).andThen(logger.logErrorsOnCall[Empty])
        }
      )
    }
  }

  override def close(): Unit = ()

}

object ApiParticipantPruningService {
  def createApiService(
      readBackend: IndexParticipantPruningService with LedgerEndService,
      writeBackend: WriteParticipantPruningService
  )(implicit grpcExecutionContext: ExecutionContext, logCtx: LoggingContext)
    : ParticipantPruningServiceGrpc.ParticipantPruningService with GrpcApiService =
    new ApiParticipantPruningService(readBackend, writeBackend)

}
