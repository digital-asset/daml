// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.{Duration, Instant}
import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.dec.{DirectExecutionContext => DEC}
import com.daml.ledger.participant.state.v1.Update.{ConfigurationChangeRejected, ConfigurationChanged}
import com.daml.ledger.participant.state.v1.{Configuration, Offset, ReadService, SubmissionId, SubmissionResult, Update, WriteService}
import com.daml.lf.data.Time.Timestamp
import org.slf4j.LoggerFactory

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/**
 *
 * @param maxUpdates Maximum number of ReadService updates to inspect
 * @param initialDelay Delay until the first config change is submitted
 * @param retryDelay Delay between submission retries
 * @param submissionTimeout Timeout until a submission is declared lost
 * @param maxAttempts Maximum number of submission attempts
 */
case class LedgerInitializationConfig(
  maxUpdates: Long,
  initialDelay: Duration,
  retryDelay: Duration,
  submissionTimeout: Duration,
  maxAttempts: Long,
)

object LedgerInitializationConfig {
  def reasonableDefault: LedgerInitializationConfig = LedgerInitializationConfig(
    maxUpdates = 10,
    initialDelay = Duration.ofSeconds(2),
    retryDelay = Duration.ofSeconds(1),
    submissionTimeout = Duration.ofSeconds(60),
    maxAttempts = 5,
  )
}

object LedgerInitialization {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def initialize(
      readService: ReadService,
      writeService: WriteService,
      initialConfig: Configuration,
      config: LedgerInitializationConfig = LedgerInitializationConfig.reasonableDefault,
  )(implicit mat: Materializer): Future[Configuration] = {

    // Mutable state
    object State {
      var submittedId: Option[SubmissionId] = None
      var attempt = 1
      var skippedUpdates = 0
      var retryDelay: Duration = config.retryDelay
      var nextSubmission: Instant = Instant.now.plus(config.initialDelay)
      var submissionTimeout: Instant = Instant.EPOCH

      def isCurrentSubmission(submissionId: SubmissionId): Boolean =
        submittedId.contains(submissionId)

      def shouldSubmitNow(): Boolean = {
        val now = Instant.now
        (submittedId.isEmpty && now.isAfter(nextSubmission)) || (submittedId.isDefined && now
          .isAfter(submissionTimeout))
      }

      def scheduleRetry(reason: String): Future[Option[Configuration]] = {
        if (attempt < config.maxAttempts) {
          submittedId = None
          nextSubmission = Instant.now.plus(retryDelay)
          attempt = attempt + 1
          retryDelay = retryDelay.multipliedBy(2)
          logger.debug(s"$reason Scheduling attempt $attempt/${config.maxAttempts} at $nextSubmission.")
          Future.successful(None)
        } else {
          logger.debug(s"$reason No more retries left.")
          Future.failed(new RuntimeException("Maximum number of retries reached"))
        }
      }

      def registerSubmission(
          submissionId: SubmissionId,
          timeout: Instant): Future[Option[Configuration]] = {
        State.submittedId = Some(submissionId)
        submissionTimeout = timeout
        Future.successful(None)
      }

      def unrelatedUpdate(): Future[Option[Configuration]] = {
        skippedUpdates = skippedUpdates + 1
        if (skippedUpdates > config.maxAttempts)
          Future.failed(new RuntimeException("Maximum number of ledger updates reached"))
        else
          Future.successful(None)
      }
    }

    type InputStreamElement = Either[(Offset, Update), Unit]
    val source1: Source[InputStreamElement, NotUsed] =
      readService.stateUpdates(None).map(Left(_))
    val source2: Source[InputStreamElement, _] = Source.tick(100.millis, 100.millis, Right(()))

    // Aproach:
    // - Merge state updates with regular ticks to guarantee progress
    // - Process stream elements:
    //   - Submit a config change if required (no config found so far, or previous submission failed)
    //   - Terminate if we get a configuration changed entry
    //   - Terminate if we inspect more than maxUpdates entries
    //   - Terminate if we there are too many submission retries
    source1
      .merge(source2)
      .mapAsync(1) {
        case Left((_, ConfigurationChanged(_, _, _, newConfig))) =>
          Future.successful(Some(newConfig))
        case Left((_, ConfigurationChangeRejected(_, sid, _, _, reason))) =>
          if (State.isCurrentSubmission(sid)) {
            State.scheduleRetry(s"Configuration change rejected. Reason: $reason.")
          } else
            Future.successful(None)
        case Left((_, _)) =>
          State.unrelatedUpdate()
        case Right(_) =>
          if (State.shouldSubmitNow()) {
            val submissionId = SubmissionId.assertFromString(UUID.randomUUID().toString)
            val timeout = Instant.now.plus(config.submissionTimeout)
            writeService
              .submitConfiguration(
                Timestamp.assertFromInstant(timeout),
                submissionId,
                initialConfig,
              )
              .toScala
              .transformWith {
                case Success(SubmissionResult.Acknowledged) =>
                  State.registerSubmission(submissionId, timeout)
                case Success(result) =>
                  State.scheduleRetry(
                    s"Configuration change submission failed. Reason: ${result.description}.")
                case Failure(error) =>
                  State.scheduleRetry(s"Configuration change submission failed. Reason: $error.")
              }(DEC)
          } else
            Future.successful(None)
      }
      .collect { case Some(x) => x }
      .runWith(Sink.head)
  }
}
