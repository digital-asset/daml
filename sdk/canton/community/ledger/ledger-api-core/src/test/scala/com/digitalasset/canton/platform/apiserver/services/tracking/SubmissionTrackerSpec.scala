// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.error.{ContextualizedErrorLogger, ErrorsAssertions}
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors
import com.digitalasset.canton.ledger.error.{CommonErrors, LedgerApiErrors}
import com.digitalasset.canton.logging.LedgerErrorLoggingContext
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker.{
  SubmissionKey,
  SubmissionTrackerImpl,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, config}
import com.google.rpc.status.Status
import io.grpc.StatusRuntimeException
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{Assertion, Succeeded}

import java.util.Timer
import scala.concurrent.{Future, Promise}
import scala.util.Try

class SubmissionTrackerSpec
    extends AnyFlatSpec
    with ScalaFutures
    with ErrorsAssertions
    with IntegrationPatience
    with Eventually
    with BaseTest
    with HasExecutionContext {

  behavior of classOf[SubmissionTracker].getSimpleName

  it should "track a submission by correct SubmissionKey" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit
      // Track new submission
      trackedSubmissionF = submissionTracker.track(submissionKey, `1 day timeout`, submitSucceeds)

      // Completion with mismatching submissionId
      completionWithMismatchingSubmissionId = completionOk.copy(submissionId = "wrongSubmissionId")
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(completion =
          Some(completionWithMismatchingSubmissionId)
        ) -> submitters
      )

      // Completion with mismatching commandId
      completionWithMismatchingCommandId = completionOk.copy(commandId = "wrongCommandId")
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(completion =
          Some(completionWithMismatchingCommandId)
        ) -> submitters
      )

      // Completion with mismatching applicationId
      completionWithMismatchingAppId = completionOk.copy(applicationId = "wrongAppId")
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(completion = Some(completionWithMismatchingAppId)) -> submitters
      )

      // Completion with mismatching actAs
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(completion = Some(completionOk)) -> (submitters + "another_party")
      )

      // Matching completion
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(completion = Some(completionOk)) -> submitters
      )

      trackedSubmission <- trackedSubmissionF
    } yield {
      trackedSubmission shouldBe CompletionResponse(None, completionOk)
    }
  }

  it should "fail on submission failure" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit
      // Track new submission
      trackedSubmissionF = submissionTracker.track(submissionKey, `1 day timeout`, submitFails)

      failure <- trackedSubmissionF.failed
    } yield {
      failure shouldBe a[RuntimeException]
      failure.getMessage shouldBe failureInSubmit.getMessage
    }
  }

  it should "fail on completion failure" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit
      // Track new submission
      trackedSubmissionF = submissionTracker.track(submissionKey, `1 day timeout`, submitSucceeds)

      // Complete the submission with a failed completion
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(
          completion = Some(completionFailed),
          checkpoint = None,
        ) -> submitters
      )

      failure <- trackedSubmissionF.failed
    } yield inside(failure) { case sre: StatusRuntimeException =>
      assertError(
        sre,
        completionFailedGrpcCode,
        completionFailedMessage,
        Seq.empty,
        verifyEmptyStackTrace = false,
      )
      succeed
    }
  }

  it should "fail if timeout reached" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] =
      submissionTracker
        .track(submissionKey, zeroTimeout, submitSucceeds)
        .failed
        .map(inside(_) { case actualStatusRuntimeException: StatusRuntimeException =>
          assertError(
            actual = actualStatusRuntimeException,
            expected = CommonErrors.RequestTimeOut
              .Reject(
                "Timed out while awaiting for a completion corresponding to a command submission with command-id=cId_1 and submission-id=sId_1.",
                definiteAnswer = false,
              )
              .asGrpcError,
          )
          succeed
        })
  }

  it should "fail on duplicate submission" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit

      // Track new submission
      firstSubmissionF = submissionTracker.track(submissionKey, `1 day timeout`, submitSucceeds)

      // Track the same submission again
      actualException <- submissionTracker
        .track(submissionKey, `1 day timeout`, submitSucceeds)
        .failed

      // Complete the first submission to ensure clean pending map at the end
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(completion = Some(completionOk), checkpoint = None) -> submitters
      )
      _ <- firstSubmissionF
    } yield inside(actualException) { case actualStatusRuntimeException: StatusRuntimeException =>
      // Expect duplicate error
      assertError(
        actual = actualStatusRuntimeException,
        expected = ConsistencyErrors.DuplicateCommand
          .Reject(existingCommandSubmissionId = Some(submissionId))
          .asGrpcError,
      )
      succeed
    }
  }

  it should "fail on a submission with a command missing the submission id" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] =
      loggerFactory.assertLogs(
        within = {
          submissionTracker
            .track(
              submissionKey = submissionKey.copy(submissionId = ""),
              timeout = `1 day timeout`,
              submit = submitSucceeds,
            )
            .failed
            .map(inside(_) { case actualStatusRuntimeException: StatusRuntimeException =>
              assertError(
                actual = actualStatusRuntimeException,
                expected = CommonErrors.ServiceInternalError
                  .Generic("Missing submission id in submission tracker")
                  .asGrpcError,
              )
              succeed
            })
        },
        assertions = _.errorMessage should include(
          "SERVICE_INTERNAL_ERROR(4,0): Missing submission id in submission tracker"
        ),
        _.errorMessage should include(
          "SERVICE_INTERNAL_ERROR(4,0): Missing submission id in submission tracker"
        ),
      )
  }

  it should "gracefully handle errors in the cancellable timeout creation" in new SubmissionTrackerFixture {
    private lazy val thrownException = new RuntimeException("scheduleOnce throws")
    override def timeoutSupport: CancellableTimeoutSupport = new CancellableTimeoutSupport {
      override def scheduleOnce[T](
          duration: config.NonNegativeFiniteDuration,
          promise: Promise[T],
          onTimeout: => Try[T],
      )(implicit traceContext: TraceContext): AutoCloseable =
        throw thrownException
    }
    override def run: Future[Assertion] = for {
      _ <- Future.unit
      // Track new submission
      trackedSubmissionF = loggerFactory.suppressErrors(
        submissionTracker.track(submissionKey, `1 day timeout`, submitFails)
      )
      failure <- trackedSubmissionF.failed
    } yield {
      failure shouldBe thrownException
    }
  }

  it should "fail after exceeding the max-commands-in-flight" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit

      _ = submissionTracker.track(
        submissionKey.copy(commandId = "c1"),
        `1 day timeout`,
        submitSucceeds,
      )
      _ = submissionTracker.track(
        submissionKey.copy(commandId = "c2"),
        `1 day timeout`,
        submitSucceeds,
      )
      _ = submissionTracker.track(
        submissionKey.copy(commandId = "c3"),
        `1 day timeout`,
        submitSucceeds,
      )
      // max-commands-in-flight = 3. Expect rejection
      submissionOverLimitF = submissionTracker.track(
        submissionKey.copy(commandId = "c4"),
        `1 day timeout`,
        submitSucceeds,
      )

      // Close the tracker to ensure clean pending map at the end
      _ = submissionTracker.close()
      failure <- submissionOverLimitF.failed
    } yield inside(failure) { case actualStatusRuntimeException: StatusRuntimeException =>
      assertError(
        actual = actualStatusRuntimeException,
        expected = LedgerApiErrors.ParticipantBackpressure
          .Rejection("Maximum number of commands in-flight reached")
          .asGrpcError,
      )
      succeed
    }
  }

  it should "fail if a command completion is missing its completion status" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = loggerFactory.suppressErrors {
      for {
        _ <- Future.unit
        // Track new submission
        trackedSubmissionF = submissionTracker.track(submissionKey, `1 day timeout`, submitSucceeds)

        // Complete the submission with completion response
        _ = submissionTracker.onCompletion(
          CompletionStreamResponse(
            completion = Some(completionOk.copy(status = None)),
            checkpoint = None,
          ) -> submitters
        )

        failure <- trackedSubmissionF.failed
      } yield inside(failure) { case ex: StatusRuntimeException =>
        assertError(
          actual = ex,
          expected = CommonErrors.ServiceInternalError
            .Generic("Command completion is missing completion status")
            .asGrpcError,
        )
        succeed
      }
    }
  }

  it should "cancel all trackers on close" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit
      // Track some submissions
      submission1 = submissionTracker.track(submissionKey, `1 day timeout`, submitSucceeds)
      submission2 = submissionTracker.track(otherSubmissionKey, `1 day timeout`, submitSucceeds)

      // Close the tracker
      _ = submissionTracker.close()

      failure1 <- submission1.failed
      failure2 <- submission2.failed
    } yield {
      inside(failure1) { case actualStatusRuntimeException: StatusRuntimeException =>
        assertError(
          actual = actualStatusRuntimeException,
          expected = CommonErrors.ServerIsShuttingDown.Reject().asGrpcError,
        )
      }
      inside(failure2) { case actualStatusRuntimeException: StatusRuntimeException =>
        assertError(
          actual = actualStatusRuntimeException,
          expected = CommonErrors.ServerIsShuttingDown.Reject().asGrpcError,
        )
      }
      succeed
    }
  }

  it should "gracefully complete the completion promises on races" in new SubmissionTrackerFixture {
    private def noConcurrentSubmissions = 100
    private def concurrentSubmissionKeys =
      (1 to noConcurrentSubmissions).map(id => submissionKey.copy(commandId = s"cmd-$id"))

    override def run: Future[Assertion] = for {
      _ <- Future.unit
      submissionTracker = new SubmissionTrackerImpl(
        timeoutSupport,
        maxCommandsInFlight = 100,
        Metrics.ForTesting,
        loggerFactory,
      )
      // Track concurrent submissions
      submissions = concurrentSubmissionKeys.map(sk =>
        sk -> submissionTracker.track(sk, `1 day timeout`, submitSucceeds)
      )

      onCompletions = submissions.map { case (key, _) =>
        () =>
          Future {
            submissionTracker.onCompletion(
              CompletionStreamResponse(completion =
                Some(completionOk.copy(commandId = key.commandId))
              ) -> submitters
            )
          }
      }

      (firstHalfOnComplete, secondHalfOnComplete) = onCompletions.splitAt(
        noConcurrentSubmissions / 2
      )

      f1 = Future.traverse(firstHalfOnComplete)(_.apply())
      s_close = Future(submissionTracker.close())
      f2 = Future.traverse(secondHalfOnComplete)(_.apply())

      _ <- f2
      _ <- s_close
      _ <- f1
      _ <- Future.traverse(submissions)(
        _._2
          .map(_ => ())
          .recover(inside(_) { case actualStatusRuntimeException: StatusRuntimeException =>
            assertError(
              actual = actualStatusRuntimeException,
              expected = CommonErrors.ServerIsShuttingDown.Reject().asGrpcError,
            )
          })
      )
    } yield {
      succeed
    }
  }

  abstract class SubmissionTrackerFixture extends BaseTest with Eventually {
    private val timer = new Timer("test-timer")
    def timeoutSupport: CancellableTimeoutSupport =
      new CancellableTimeoutSupportImpl(timer, loggerFactory)
    val submissionTracker =
      new SubmissionTrackerImpl(
        timeoutSupport,
        maxCommandsInFlight = 3,
        Metrics.ForTesting,
        loggerFactory,
      )

    val zeroTimeout: config.NonNegativeFiniteDuration = config.NonNegativeFiniteDuration.Zero
    val `1 day timeout`: config.NonNegativeFiniteDuration =
      config.NonNegativeFiniteDuration.ofDays(1L)

    val submissionId = "sId_1"
    val commandId = "cId_1"
    val applicationId = "apId_1"
    val actAs: Seq[String] = Seq("p1", "p2")
    val party = "p3"
    val submissionKey: SubmissionKey = SubmissionKey(
      submissionId = submissionId,
      commandId = commandId,
      applicationId = applicationId,
      parties = Set(party) ++ actAs,
    )
    val otherSubmissionKey: SubmissionKey = submissionKey.copy(commandId = "cId_2")
    val failureInSubmit = new RuntimeException("failure in submit")
    val submitFails: TraceContext => Future[Any] = _ => Future.failed(failureInSubmit)
    val submitSucceeds: TraceContext => Future[Any] = _ => Future.successful(())

    val submitters: Set[String] = (actAs :+ party).toSet

    val completionOk: Completion = Completion(
      submissionId = submissionId,
      commandId = commandId,
      status = Some(Status(code = io.grpc.Status.Code.OK.value())),
      applicationId = applicationId,
    )

    val errorLogger: ContextualizedErrorLogger =
      LedgerErrorLoggingContext(logger, Map(), traceContext, submissionId)

    val completionFailedGrpcCode = io.grpc.Status.Code.NOT_FOUND
    val completionFailedMessage: String = "ledger rejection"
    val completionFailed: Completion = completionOk.copy(
      status = Some(
        Status(code = completionFailedGrpcCode.value(), message = completionFailedMessage)
      )
    )

    def run: Future[Assertion]

    run.futureValue shouldBe Succeeded
    // We want to assert this for each test
    // Completion of futures might race with removal of the entries from the map
    eventually {
      submissionTracker.pending shouldBe empty
    }
    // Stop the timer
    timer.purge()
    timer.cancel()
  }
}
