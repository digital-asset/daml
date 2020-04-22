// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.time.{DateTimeException, Instant}
import java.util.UUID
import java.util.concurrent.CompletionStage

import com.daml.api.util.TimeProvider
import com.daml.dec.{DirectExecutionContext => DE}
import com.daml.ledger.api.v1.admin.participant_pruning_service.{
  ParticipantPruningServiceGrpc,
  PruneByOffsetRequest,
  PruneByTimeRequest,
  PruneResponse
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.participant.state.index.v2.IndexParticipantPruningService
import com.daml.ledger.participant.state.v1.{
  Offset,
  ParticipantPruned,
  SubmissionId,
  WriteParticipantPruningService
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.ApiOffset
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

final class ApiParticipantPruningService private (
    readBackend: IndexParticipantPruningService,
    writeBackend: WriteParticipantPruningService,
    cannotPruneMoreRecentlyThan: FiniteDuration,
    timeProvider: TimeProvider)(implicit logCtx: LoggingContext)
    extends ParticipantPruningServiceGrpc.ParticipantPruningService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    ParticipantPruningServiceGrpc.bindService(this, DE)

  override def pruneByOffset(request: PruneByOffsetRequest): Future[PruneResponse] = {
    val pruneUpToOffset: Either[StatusRuntimeException, Offset] = for {
      pruneUpToProto <- request.pruneUpTo.toRight(
        ErrorFactories.invalidArgument("prune_up_to not specified"))

      pruneUpToString <- pruneUpToProto.value match {
        case LedgerOffset.Value.Absolute(offset) => Right(offset)
        case LedgerOffset.Value.Boundary(boundary) =>
          Left(
            ErrorFactories.invalidArgument(
              s"prune_before needs to be absolute and not a boundary ${boundary.toString}"))
        case LedgerOffset.Value.Empty =>
          Left(
            ErrorFactories.invalidArgument(
              s"prune_before needs to be absolute and not empty ${pruneUpToProto.value}"))
      }

      pruneUpTo <- Ref.HexString
        .fromString(pruneUpToString)
        .left
        .map(err =>
          ErrorFactories.invalidArgument(
            s"prune_before needs to be a hexadecimal string and not ${pruneUpToString}: ${err}"))
        .map(Offset.fromHexString)

      // TODO(oliver): come up with a way to ensure that the pruning offset is not too new (as equivalent of
      //  maxPruningLimit in pruneByTime below).
    } yield pruneUpTo

    pruneUpToOffset.fold(
      Future.failed,
      pruneUpTo =>
        prune(pruneUpTo.toString, request.submissionId, writeBackend.pruneByOffset(pruneUpTo))
    )
  }

  override def pruneByTime(request: PruneByTimeRequest): Future[PruneResponse] = {
    val pruneUpToTimestamp: Either[StatusRuntimeException, Timestamp] = for {
      pruneUpToProto <- request.pruneUpTo.toRight(
        ErrorFactories.invalidArgument("prune_up_to not specified"))

      pruneUpToInstant <- Try(
        Instant
          .ofEpochSecond(pruneUpToProto.seconds, pruneUpToProto.nanos.toLong)).toEither.left
        .map {
          case t: ArithmeticException =>
            ErrorFactories.invalidArgument(
              s"arithmetic overflow converting prune_up_to to instant: ${t.getMessage}")
          case t: DateTimeException =>
            ErrorFactories.invalidArgument(
              s"timestamp exceeds min or max of instant: ${t.getMessage}")
          case t => ErrorFactories.internal(t.toString)
        }

      pruneUpTo <- Timestamp
        .fromInstant(pruneUpToInstant)
        .left
        .map(e => ErrorFactories.invalidArgument(s"failed to convert prune_up_to to timestamp $e"))

      now = timeProvider.getCurrentTime

      maxPruningLimit <- Try(
        now
          .minusMillis(cannotPruneMoreRecentlyThan.toMillis)
          .minusNanos(cannotPruneMoreRecentlyThan.toNanos)).toEither.left.map(t =>
        ErrorFactories.internal(
          s"failed to adjust current time by pruning safety duration ${t.toString}"))

      _ <- Either.cond(
        !maxPruningLimit.isBefore(pruneUpToInstant),
        (),
        ErrorFactories.invalidArgument(
          s"prune_before $pruneUpToInstant is too recent. You can prune at most at $maxPruningLimit")
      )

    } yield pruneUpTo

    pruneUpToTimestamp.fold(
      Future.failed,
      pruneUpTo =>
        prune(pruneUpTo.toString, request.submissionId, writeBackend.pruneByTime(pruneUpTo))
    )
  }

  private def prune(
      pruneUpTo: String,
      submissionIdString: String,
      pruneLedgerParticipant: => CompletionStage[Option[ParticipantPruned]])
    : Future[PruneResponse] = {
    implicit val ec: ExecutionContext = DE
    val submissionId =
      if (submissionIdString.isEmpty)
        SubmissionId.assertFromString(UUID.randomUUID().toString)
      else
        SubmissionId.assertFromString(submissionIdString)

    LoggingContext.withEnrichedLoggingContext("submissionId" -> submissionId) { implicit logCtx =>
      logger.info(s"About to prune participant up to ${pruneUpTo}")

      (for {
        ledgerPruned <- FutureConverters.toScala(pruneLedgerParticipant)

        pruneResponse <- ledgerPruned.fold[Future[PruneResponse]](
          Future.failed(ErrorFactories.participantPruningPointNotFound(
            s"Cannot prune participant at ${pruneUpTo}"))) {
          case pp @ ParticipantPruned(prunedUpToInclusiveOffset, _) =>
            readBackend
              .pruneByOffset(prunedUpToInclusiveOffset)
              .map(_ =>
                PruneResponse(
                  prunedOffset = Some(LedgerOffset(LedgerOffset.Value.Absolute(
                    ApiOffset.toApiString(pp.prunedUpToInclusiveOffset)))),
                  stateRetainedUntilOffset = pp.stateRetainedUntilOffset.map(offset =>
                    LedgerOffset(LedgerOffset.Value.Absolute(ApiOffset.toApiString(offset))))
              ))
        }
      } yield pruneResponse).andThen(logger.logErrorsOnCall[PruneResponse])(DE)

    }
  }
}

object ApiParticipantPruningService {
  def createApiService(
      readBackend: IndexParticipantPruningService,
      writeBackend: WriteParticipantPruningService,
      cannotPruneMoreRecentlyThan: FiniteDuration,
      timeProvider: TimeProvider
  )(implicit logCtx: LoggingContext)
    : ParticipantPruningServiceGrpc.ParticipantPruningService with GrpcApiService =
    new ApiParticipantPruningService(
      readBackend,
      writeBackend,
      cannotPruneMoreRecentlyThan,
      timeProvider)

}
