package com.daml.ledger.validator

import java.time.Instant

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.validator.batch.{
  BatchedSubmissionValidator,
  BatchedSubmissionValidatorFactory
}
import com.daml.logging.LoggingContext.newLoggingContext

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class BatchedValidatingCommitter[LogResult](
    now: () => Instant,
    validator: BatchedSubmissionValidator[LogResult],
    keySerializationStrategy: StateKeySerializationStrategy
)(implicit materializer: Materializer) {
  def commit(
      correlationId: String,
      envelope: Bytes,
      submittingParticipantId: ParticipantId,
      ledgerStateOperations: LedgerStateOperations[LogResult]
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult] =
    newLoggingContext("correlationId" -> correlationId) { implicit logCtx =>
      val (ledgerStateReader, commitStrategy) = BatchedSubmissionValidatorFactory
        .readerAndCommitStrategyFrom(ledgerStateOperations, keySerializationStrategy)
      validator
        .validateAndCommit(
          envelope,
          correlationId,
          now(),
          submittingParticipantId,
          ledgerStateReader,
          commitStrategy
        )
        .transformWith {
          case Success(_) =>
            Future.successful(SubmissionResult.Acknowledged)
          case Failure(exception) =>
            Future.successful(SubmissionResult.InternalError(exception.getLocalizedMessage))
        }
    }
}

object BatchedValidatingCommitter {
  def apply[LogResult](now: () => Instant, validator: BatchedSubmissionValidator[LogResult])(
      implicit materializer: Materializer): BatchedValidatingCommitter[LogResult] = {
    new BatchedValidatingCommitter[LogResult](now, validator, DefaultStateKeySerializationStrategy)
  }
}
