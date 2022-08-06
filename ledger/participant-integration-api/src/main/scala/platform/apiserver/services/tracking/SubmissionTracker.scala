// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import java.time.Duration

import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.google.protobuf.empty.Empty
import com.google.rpc.status

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

// TODO fix tests (or keep them deleted)
// TODO local performance test for several scenarios
//  mass timeouts, mass submitandwait, mass submitandwait with other completions
//  how much TP this can sustain, what is the CPU cost
// TODO measure difference in conformance test runtime
// TODO measure difference in LR task where we populate the db

trait SubmissionTracker extends AutoCloseable {
  def track(
      commands: Commands,
      timeout: Duration,
      submit: SubmitRequest => Future[Empty],
  )(implicit
      loggingContext: LoggingContext,
      errorLogger: ContextualizedErrorLogger,
  ): Future[CompletionResponse]
  def update(completionResult: CompletionStreamResponse): Unit
}

case class SubmissionKey(commandId: String, submissionId: String)

object SubmissionKey {
  def fromCompletion(completion: com.daml.ledger.api.v1.completion.Completion): SubmissionKey =
    SubmissionKey(
      commandId = completion.commandId,
      submissionId = completion.submissionId,
    )
}

case class CompletionResponse(
    checkpoint: Option[com.daml.ledger.api.v1.command_completion_service.Checkpoint],
    completion: com.daml.ledger.api.v1.completion.Completion,
)

object CompletionResponse {
  def fromCompletion(
      completion: com.daml.ledger.api.v1.completion.Completion,
      checkpoint: Option[com.daml.ledger.api.v1.command_completion_service.Checkpoint],
  ): Try[CompletionResponse] =
    completion.status match {
      case Some(completionStatus) if completionStatus.code != 0 =>
        Failure(
          io.grpc.protobuf.StatusProto.toStatusRuntimeException(
            status.Status.toJavaProto(completionStatus)
          )
        )

      case _ =>
        Success(
          CompletionResponse(
            checkpoint = checkpoint,
            completion = completion,
          )
        )
    }

  def timeout(implicit loggingContext: ContextualizedErrorLogger): Try[CompletionResponse] =
    Failure(
      LedgerApiErrors.RequestTimeOut
        .Reject(
          "Timed out while awaiting for a completion corresponding to a command submission.",
          definiteAnswer = false,
        )
        .asGrpcError
    )

  def duplicate(
      submissionId: String
  )(implicit loggingContext: ContextualizedErrorLogger): Try[CompletionResponse] =
    Failure(
      LedgerApiErrors.ConsistencyErrors.DuplicateCommand
        .Reject(
          existingCommandSubmissionId = Some(submissionId)
        )
        .asGrpcError
    )

  def closing(implicit loggingContext: ContextualizedErrorLogger): Try[CompletionResponse] =
    Failure(LedgerApiErrors.ServerIsShuttingDown.Reject().asGrpcError)
}

object SubmissionTracker {

  def owner: ResourceOwner[SubmissionTracker] = {
    TimeoutSupport
      .owner("submission-tracker-timeout-timer")
      .map(new SubmissionTrackerImpl(_))
      .map(() => _)
      .flatMap(ResourceOwner.forCloseable)
  }

  class TrackingEntry(onFinish: () => Unit) {
    private val completionPromise = Promise[CompletionResponse]()
    val result: Future[CompletionResponse] = completionPromise.future
    def finish(result: Try[CompletionResponse]): Unit = {
      completionPromise.tryComplete(result)
      onFinish()
    }
  }

  class SubmissionTrackerImpl(timeoutSupport: TimeoutSupport) extends SubmissionTracker {
    private val pending = TrieMap[SubmissionKey, TrackingEntry]()
    private val logger = ContextualizedLogger.get(this.getClass)
    private val errorLogger = DamlContextualizedErrorLogger.forClass(getClass)

    override def track(
        commandsPotentiallyWithoutSubmissionId: Commands,
        timeout: Duration,
        submit: SubmitRequest => Future[Empty],
    )(implicit
        loggingContext: LoggingContext,
        errorLogger: ContextualizedErrorLogger,
    ): Future[CompletionResponse] = {
      // we need submissionId for tracking, this might be not specified, so we generate one in this case
      val commands =
        if (commandsPotentiallyWithoutSubmissionId.submissionId == "")
          commandsPotentiallyWithoutSubmissionId.copy(submissionId =
            SubmissionIdGenerator.Random.generate()
          )
        else commandsPotentiallyWithoutSubmissionId
      val submissionKey = SubmissionKey(
        commandId = commands.commandId,
        submissionId = commands.submissionId,
      )
      val startedTimeout = timeoutSupport.startTimeout(timeout)(
        onTimeout = () =>
          attemptFinish(submissionKey) {
            logger.info(
              s"Command ${submissionKey.commandId} from submission ${submissionKey.submissionId} (command timeout $timeout) timed out."
            )
            CompletionResponse.timeout
          }
      )
      val trackingEntry = new TrackingEntry(onFinish = startedTimeout.cancel)
      pending.putIfAbsent(submissionKey, trackingEntry) match {
        case Some(_) =>
          trackingEntry.finish(CompletionResponse.duplicate(submissionKey.submissionId))

        case None =>
          submit(SubmitRequest(Some(commands)))
            .onComplete {
              case Success(_) => // succeeded, nothing to do
              case Failure(throwable) =>
                // submitting command failed, finishing entry with the very same error
                attemptFinish(submissionKey)(Failure(throwable))
            }(ExecutionContext.parasitic)
      }
      trackingEntry.result
    }

    override def update(completionResult: CompletionStreamResponse): Unit =
      completionResult.completions.foreach { completion =>
        attemptFinish(SubmissionKey.fromCompletion(completion))(
          CompletionResponse.fromCompletion(completion, completionResult.checkpoint)
        )
      }

    override def close(): Unit =
      pending.keys.foreach(attemptFinish(_)(CompletionResponse.closing(errorLogger)))

    private def attemptFinish(submissionKey: SubmissionKey)(
        result: => Try[CompletionResponse]
    ): Unit =
      pending.remove(submissionKey).foreach(_.finish(result))
  }
}
