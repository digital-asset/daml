// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.{DirectExecutionContext, ExecutionContextMonitor}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.typesafe.scalalogging.Logger
import org.scalactic.source
import org.scalatest.AppendedClues.*
import org.scalatest.Assertion
import org.scalatest.Checkpoints.Checkpoint
import org.scalatest.Inside.*
import org.scalatest.Inspectors.*
import org.scalatest.matchers.should.Matchers.*
import org.slf4j.event.Level
import org.slf4j.event.Level.{ERROR, WARN}

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/** A version of [[NamedLoggerFactory]] that allows suppressing and intercepting warnings and errors in the log.
  * Intended for testing.
  *
  * Suppressed warnings and errors will still be logged, but with level `INFO` and the prefix `"Suppressed WARN: "`
  * or `"Suppressed ERROR: "`.
  *
  * The methods of this class will check whether the computation during which logging is suppressed yields a future.
  * If so, suppression will finish after completion of the future (instead of after creation of the future).
  *
  * Nested suppression / recording of log messages is not supported to keep the implementation simple.
  *
  * Only affects logging by Canton code.
  * To suppress logging in libraries configure a [[com.digitalasset.canton.logging.Rewrite]] rule in `logback-test.xml`.
  *
  * @param skipLogEntry Log entries satisfying this predicate are suppressed, but not included in the queue of suppressed log entries.
  */
class SuppressingLogger private[logging] (
    underlyingLoggerFactory: NamedLoggerFactory,
    pollTimeout: FiniteDuration,
    skipLogEntry: LogEntry => Boolean,
    activeSuppressionRule: AtomicReference[SuppressionRule] =
      new AtomicReference[SuppressionRule](SuppressionRule.NoSuppression),
    private[logging] val recordedLogEntries: java.util.concurrent.BlockingQueue[LogEntry] =
      new java.util.concurrent.LinkedBlockingQueue[LogEntry](),
) extends NamedLoggerFactory {

  /** Logger used to log internal problems of this class.
    */
  private val internalLogger: Logger = underlyingLoggerFactory.getLogger(getClass)

  private val directExecutionContext: ExecutionContext = DirectExecutionContext(internalLogger)

  private val SuppressionPrefix: String = "Suppressed %s: %s"

  override val name: String = underlyingLoggerFactory.name
  override val properties: ListMap[String, String] = underlyingLoggerFactory.properties

  override def appendUnnamedKey(key: String, value: String): NamedLoggerFactory =
    // intentionally share suppressedLevel and queues so suppression on a parent logger will effect a child and collect all suppressed messages
    new SuppressingLogger(
      underlyingLoggerFactory.appendUnnamedKey(key, value),
      pollTimeout,
      skipLogEntry,
      activeSuppressionRule,
      recordedLogEntries,
    )

  override def append(key: String, value: String): SuppressingLogger =
    // intentionally share suppressedLevel and queues so suppression on a parent logger will effect a child and collect all suppressed messages
    new SuppressingLogger(
      underlyingLoggerFactory.append(key, value),
      pollTimeout,
      skipLogEntry,
      activeSuppressionRule,
      recordedLogEntries,
    )

  override private[logging] def getLogger(fullName: String): Logger = {
    val actualLogger = underlyingLoggerFactory.getLogger(fullName)
    val suppressedMessageLogger = new BufferingLogger(recordedLogEntries, fullName, skipLogEntry(_))

    val logger =
      new SuppressingLoggerDispatcher(
        actualLogger.underlying.getName,
        suppressedMessageLogger,
        activeSuppressionRule,
        SuppressionPrefix,
      )
    logger.setDelegate(actualLogger.underlying)
    Logger(logger)
  }

  def assertThrowsAndLogs[T <: Throwable](
      within: => Any,
      assertions: (LogEntry => Assertion)*
  )(implicit c: ClassTag[T], pos: source.Position): Assertion =
    assertLogs(checkThrowable[T](the[Throwable] thrownBy within), assertions*)

  def assertThrowsAndLogsSuppressing[T <: Throwable](rule: SuppressionRule)(
      within: => Any,
      assertions: (LogEntry => Assertion)*
  )(implicit c: ClassTag[T], pos: source.Position): Assertion =
    assertLogs(rule)(checkThrowable[T](the[Throwable] thrownBy within), assertions*)

  def assertThrowsAndLogsAsync[T <: Throwable](
      within: => Future[_],
      assertion: T => Assertion,
      entryChecks: (LogEntry => Assertion)*
  )(implicit c: ClassTag[T], pos: source.Position): Future[Assertion] =
    assertLogs(
      within.transform {
        case Success(_) =>
          fail(s"An exception of type $c was expected, but no exception was thrown.")
        case Failure(c(t)) => Success(assertion(t))
        case Failure(t) => fail(s"Exception has wrong type. Expected type: $c. Got: $t.", t)
      }(directExecutionContext),
      entryChecks*
    )

  def assertThrowsAndLogsSeq[T <: Throwable](
      within: => Any,
      assertion: Seq[LogEntry] => Assertion,
  )(implicit c: ClassTag[T], pos: source.Position): Assertion =
    checkThrowable[T](assertLoggedWarningsAndErrorsSeq(the[Throwable] thrownBy within, assertion))

  def assertThrowsAndLogsUnordered[T <: Throwable](
      within: => Any,
      assertions: (LogEntry => Assertion)*
  )(implicit c: ClassTag[T], pos: source.Position): Assertion =
    checkThrowable[T](assertLogsUnordered(the[Throwable] thrownBy within, assertions*))

  private def checkThrowable[T <: Throwable](
      within: => Throwable
  )(implicit c: ClassTag[T], pos: source.Position): Assertion =
    inside(within) {
      case _: T => succeed
      case t: Throwable => fail(s"The throwable has an incorrect type ${t.getClass}.")
    }

  def assertThrowsAndLogsUnorderedOptional[T <: Throwable](
      within: => Any,
      assertions: (LogEntryOptionality, LogEntry => Assertion)*
  )(implicit c: ClassTag[T], pos: source.Position): Assertion =
    checkThrowable[T](assertLogsUnorderedOptional(the[Throwable] thrownBy within, assertions*))

  def assertInternalError[T <: Throwable](within: => Any, assertion: T => Assertion)(implicit
      c: ClassTag[T],
      pos: source.Position,
  ): Assertion = {
    assertLogs(
      {
        val t = the[T] thrownBy within
        assertion(t) withClue ErrorUtil.messageWithStacktrace(t)
      },
      checkLogsInternalError(assertion),
    )
  }

  def checkLogsInternalError[T <: Throwable](
      assertion: T => Assertion
  )(entry: LogEntry)(implicit c: ClassTag[T], pos: source.Position): Assertion = {
    entry.errorMessage shouldBe ErrorUtil.internalErrorMessage
    entry.throwable match {
      case Some(c(t)) => assertion(t) withClue ErrorUtil.messageWithStacktrace(t)
      case Some(t) => fail(s"Internal error logging throwable of wrong type. Expected type: $c.", t)
      case None => fail("Internal error not logging a throwable.")
    }
  }

  def assertInternalErrorAsync[T <: Throwable](
      within: => Future[_],
      assertion: T => Assertion,
  )(implicit c: ClassTag[T], pos: source.Position): Future[Assertion] =
    assertLogs(
      within.transform {
        case Success(_) =>
          fail(s"An exception of type $c was expected, but no exception was thrown.")
        case Failure(c(t)) => Success(assertion(t))
        case Failure(t) => fail(s"Exception has wrong type. Expected type: $c.", t)
      }(directExecutionContext),
      checkLogsInternalError(assertion),
    )

  def assertInternalErrorAsyncUS[T <: Throwable](
      within: => FutureUnlessShutdown[_],
      assertion: T => Assertion,
  )(implicit c: ClassTag[T], pos: source.Position): FutureUnlessShutdown[Assertion] =
    assertLogs(
      within.transform {
        case Success(_) =>
          fail(s"An exception of type $c was expected, but no exception was thrown.")
        case Failure(c(t)) => Success(UnlessShutdown.Outcome(assertion(t)))
        case Failure(t) => fail(s"Exception has wrong type. Expected type: $c.", t)
      }(directExecutionContext),
      checkLogsInternalError(assertion),
    )

  /** Asserts that the sequence of logged warnings/errors meets a given sequence of assertions.
    * Use this if the expected sequence of logged warnings/errors is deterministic.
    *
    * This method will automatically use asynchronous suppression if `A` is `Future[_]`.
    *
    * The method will delete logged messages up to and including the first message on which an assertion fails.
    *
    * @throws java.lang.IllegalArgumentException if `T` is `EitherT` or `OptionT`, because the method cannot detect
    *                                            whether asynchronous suppression is needed in this case.
    *                                            Use `EitherT.value` or `OptionT`.value to work around this.
    */
  def assertLogs[A](within: => A, assertions: (LogEntry => Assertion)*)(implicit
      pos: source.Position
  ): A =
    assertLogs(rule = SuppressionRule.LevelAndAbove(WARN))(within, assertions*)

  /** Asserts that the sequence of logs captured by the suppression rule meets a given sequence of assertions.
    * Use this if the expected sequence of logs is deterministic.
    *
    * This method will automatically use asynchronous suppression if `A` is `Future[_]`.
    *
    * The method will delete logged messages up to and including the first message on which an assertion fails.
    *
    * @throws java.lang.IllegalArgumentException if `T` is `EitherT` or `OptionT`, because the method cannot detect
    *                                            whether asynchronous suppression is needed in this case.
    *                                            Use `EitherT.value` or `OptionT`.value to work around this.
    */
  def assertLogs[A](
      rule: SuppressionRule
  )(within: => A, assertions: (LogEntry => Assertion)*)(implicit
      pos: source.Position
  ): A =
    suppress(rule) {
      runWithCleanup(
        {
          within
        },
        { () =>
          // check the log

          // Check that every assertion succeeds on the corresponding log entry
          forEvery(assertions) { assertion =>
            // Poll with a timeout to wait for asynchronously logged messages,
            // i.e., logged by: myFuture.onFailure{ case t => logger.error("...", t) }
            Option(recordedLogEntries.poll(pollTimeout.length, pollTimeout.unit)) match {
              case Some(logEntry) =>
                assertion(logEntry) withClue s"\n\nRemaining log entries:${LogEntry.format(recordedLogEntries.asScala)}"
              case None => fail(s"Missing log message.")
            }
          }

          // Check that there are no unexpected log entries
          if (recordedLogEntries.asScala.nonEmpty) {
            fail(s"Found unexpected log messages:${LogEntry.format(recordedLogEntries.asScala)}")
          }
        },
        () => (),
      )
    }

  /** Overload of `assertLogsSeq` that defaults to only capturing Warnings and Errors. */
  def assertLoggedWarningsAndErrorsSeq[A](within: => A, assertion: Seq[LogEntry] => Assertion): A =
    assertLogsSeq(SuppressionRule.LevelAndAbove(WARN))(within, assertion)

  /** Asserts that the sequence of logged warnings/errors meets a given assertion.
    * Use this if the expected sequence of logged warnings/errors is non-deterministic.
    *
    * On success, the method will delete all logged messages. So this method is not idempotent.
    *
    * On failure, the method will not delete any logged message to support retrying with [[com.digitalasset.canton.BaseTest#eventually]].
    *
    * This method will automatically use asynchronous suppression if `A` is `Future[_]`.
    *
    * @throws java.lang.IllegalArgumentException if `A` is `EitherT` or `OptionT`, because the method cannot detect
    *                                            whether asynchronous suppression is needed in this case.
    *                                            Use `EitherT.value` or `OptionT`.value to work around this.
    */
  def assertLogsSeq[A](
      rule: SuppressionRule
  )(within: => A, assertion: Seq[LogEntry] => Assertion): A =
    suppress(rule) {
      runWithCleanup(
        {
          within
        },
        { () => checkLogsAssertion(assertion) },
        () => (),
      )
    }

  /** Asserts that the sequence of logged warnings/errors will eventually meet a given assertion.
    * Use this if the expected sequence of logged warnings/errors is non-deterministic and the log-message assertion might not immediately succeed when it is called (e.g. because the messages might be logged with a delay).
    * The SupressingLogger only starts supresing and capturing logs when this method is called,
    * If some logs that we want to capture might fire before the start or after the end of the suppression. Please use method `assertEventuallyLogsSeq` instead to provide those action in `within` parameter.
    * On success, the method will delete all logged messages. So this method is not idempotent.
    *
    * On failure of the log-message assertion, it will be retried until it eventually succeeds or a timeout occurs.
    * On timeout without success, the method will not delete any logged message
    */
  def assertEventuallyLogsSeq_(
      rule: SuppressionRule
  )(
      assertion: Seq[LogEntry] => Assertion,
      timeUntilSuccess: FiniteDuration = 20.seconds,
      maxPollInterval: FiniteDuration = 5.seconds,
  ): Unit = assertEventuallyLogsSeq(rule)((), assertion, timeUntilSuccess, maxPollInterval)

  /** Asserts that the sequence of logged warnings/errors will eventually meet a given assertion.
    * Use this if the expected sequence of logged warnings/errors is non-deterministic and the log-message assertion might not immediately succeed when it is called (e.g. because the messages might be logged with a delay).
    *
    * On success, the method will delete all logged messages. So this method is not idempotent.
    *
    * On failure of the log-message assertion, it will be retried until it eventually succeeds or a timeout occurs.
    * On timeout without success, the method will not delete any logged message
    *
    * This method will automatically use asynchronous suppression if `A` is `Future[_]`.
    *
    * @throws java.lang.IllegalArgumentException if `A` is `EitherT` or `OptionT`, because the method cannot detect
    *                                            whether asynchronous suppression is needed in this case.
    *                                            Use `EitherT.value` or `OptionT`.value to work around this.
    */
  def assertEventuallyLogsSeq[A](
      rule: SuppressionRule
  )(
      within: => A,
      assertion: Seq[LogEntry] => Assertion,
      timeUntilSuccess: FiniteDuration = 20.seconds,
      maxPollInterval: FiniteDuration = 5.seconds,
  ): A =
    suppress(rule) {
      runWithCleanup(
        { within },
        { () =>
          BaseTest.eventually(timeUntilSuccess, maxPollInterval)(checkLogsAssertion(assertion))
        },
        () => (),
      )
    }

  private def checkLogsAssertion(assertion: Seq[LogEntry] => Assertion): Unit = {
    // check the log
    val logEntries = recordedLogEntries.asScala.toSeq
    assertion(logEntries)
    // Remove checked log entries only if check succeeds.
    // This is to allow for retries, if the check fails.
    logEntries.foreach(_ => recordedLogEntries.remove())
  }

  /** Asserts that the sequence of logged warnings/errors meets a set of expected log messages. */
  def assertLogsSeqString[A](rule: SuppressionRule, expectedLogs: Seq[String])(within: => A): A =
    assertLogsSeq(rule)(
      within,
      logs =>
        forEvery(logs)(log =>
          assert(
            expectedLogs.exists(msg => log.message.contains(msg)),
            s"line $log contained unexpected log",
          )
        ),
    )

  /** Asserts that the sequence of logged warnings/errors matches a set of assertions.
    * Use this if the order of logged warnings/errors is nondeterministic.
    * Matching the assertions against the logged messages is sequential in the order of the assertions.
    * That is, more specific assertions must be listed earlier.
    *
    * This method will automatically use asynchronous suppression if `A` is `Future[_]`.
    *
    * The method will delete as many logged messages as there are assertions.
    *
    * @throws java.lang.IllegalArgumentException if `T` is `EitherT` or `OptionT`, because the method cannot detect
    *                                            whether asynchronous suppression is needed in this case.
    *                                            Use `EitherT.value` or `OptionT`.value to work around this.
    */
  def assertLogsUnordered[A](within: => A, assertions: (LogEntry => Assertion)*)(implicit
      pos: source.Position
  ): A =
    assertLogsUnorderedOptional(within, assertions.map(LogEntryOptionality.Required -> _)*)

  /** Asserts that the sequence of logged warnings/errors matches a set of assertions.
    * Use this if the order of logged warnings/errors is nondeterministic and some of them are optional.
    * Matching the assertions against the logged messages is sequential in the order of the assertions.
    * That is, more specific assertions must be listed earlier.
    *
    * This method will automatically use asynchronous suppression if `A` is `Future[_]`.
    *
    * The method will delete as many logged messages as there are assertions.
    *
    * @throws java.lang.IllegalArgumentException if `T` is `EitherT` or `OptionT`, because the method cannot detect
    *                                            whether asynchronous suppression is needed in this case.
    *                                            Use `EitherT.value` or `OptionT`.value to work around this.
    */
  def assertLogsUnorderedOptional[A](
      within: => A,
      assertions: (LogEntryOptionality, LogEntry => Assertion)*
  )(implicit pos: source.Position): A =
    suppress(SuppressionRule.LevelAndAbove(WARN)) {
      runWithCleanup(
        within,
        () => {
          val unmatchedAssertions =
            mutable.SortedMap[Int, (LogEntryOptionality, LogEntry => Assertion)]() ++
              assertions.zipWithIndex.map { case (assertion, index) => index -> assertion }
          val matchedLogEntries = mutable.ListBuffer[(Int, LogEntry)]()
          val unexpectedLogEntries = mutable.ListBuffer[LogEntry]()

          @tailrec
          def checkLogEntries(): Unit =
            Option(recordedLogEntries.poll(pollTimeout.length, pollTimeout.unit)) match {
              case Some(logEntry) =>
                unmatchedAssertions.collectFirst {
                  case (index, (required, assertion)) if Try(assertion(logEntry)).isSuccess =>
                    (required, index)
                } match {
                  case Some((LogEntryOptionality.OptionalMany, index)) =>
                    matchedLogEntries += index -> logEntry
                  case Some((_, index)) =>
                    unmatchedAssertions.remove(index).discard
                    matchedLogEntries += index -> logEntry
                  case None => unexpectedLogEntries += logEntry
                }
                checkLogEntries()
              case None => ()
            }

          checkLogEntries()

          val errors = mutable.ListBuffer[String]()

          if (unexpectedLogEntries.nonEmpty) {
            errors += s"Found unexpected log messages:${LogEntry.format(unexpectedLogEntries)}"
          }

          val requiredUnmatchedAssertions = unmatchedAssertions.filter { case (_, (required, _)) =>
            required == LogEntryOptionality.Required
          }
          if (requiredUnmatchedAssertions.nonEmpty) {
            errors += s"No log message has matched the assertions with index ${requiredUnmatchedAssertions.keys
                .mkString(", ")}.\n"
          }

          if (errors.nonEmpty) {
            errors += s"Matched log messages:${matchedLogEntries
                .map { case (index, entry) => s"$index:\t$entry" }
                .mkString("\n", "\n", "\n")}"

            fail(errors.mkString("\n"))
          }
        },
        () => (),
      )
    }

  def numberOfRecordedEntries: Int = recordedLogEntries.size()

  def pollRecordedLogEntry(timeout: FiniteDuration = pollTimeout): Option[LogEntry] =
    Option(recordedLogEntries.poll(timeout.length, timeout.unit))

  def fetchRecordedLogEntries: Seq[LogEntry] = recordedLogEntries.asScala.toSeq

  /** Use this only in very early stages of development.
    * Try to use [[assertLogs]] instead which lets you specify the specific messages that you expected to suppress.
    * This avoids the risk of hiding unrelated warnings and errors.
    */
  def suppressWarnings[A](within: => A): A = suppress(SuppressionRule.Level(WARN))(within)

  /** Use this only in very early stages of development.
    * Try to use [[assertLogs]] instead which lets you specify the specific messages that you expected to suppress.
    * This avoids the risk of hiding unrelated warnings and errors.
    */
  def suppressWarningsAndErrors[A](within: => A): A =
    suppress(SuppressionRule.LevelAndAbove(WARN))(within)

  /** Use this only in very early stages of development.
    * Try to use [[assertLogs]] instead which lets you specify the specific messages that you expected to suppress.
    * This avoids the risk of hiding unrelated warnings and errors.
    */
  def suppressErrors[A](within: => A): A = suppress(SuppressionRule.Level(ERROR))(within)

  /** Use this only in very early stages of development.
    * Try to use [[assertThrowsAndLogs]] instead which lets you specify the specific messages that you expected to suppress.
    * This avoids the risk of hiding unrelated warnings and errors.
    */
  def suppressWarningsErrorsExceptions[T <: Throwable](
      within: => Any
  )(implicit classTag: ClassTag[T], pos: source.Position): Any = suppressWarningsAndErrors {
    a[T] should be thrownBy within
  }

  def suppress[A](rule: SuppressionRule)(within: => A): A = {
    val endSuppress = beginSuppress(rule)
    runWithCleanup(within, () => (), endSuppress)
  }

  /** First runs `body`, `onSuccess`, and then `doFinally`.
    * Runs `onSuccess` after `body` if `body` completes successfully
    * Runs `doFinally` after `body` and `onSuccess`, even if they terminate abruptly.
    *
    * @return The result of `body`. If `body` is of type `Future[_]`, a future with the same result as `body` that
    *         completes after completion of `doFinally`.
    * @throws java.lang.IllegalArgumentException if `T` is `EitherT` or `OptionT`
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.AsInstanceOf"))
  private def runWithCleanup[T](body: => T, onSuccess: () => Unit, doFinally: () => Unit): T = {
    var isAsync = false
    try {
      // Run the computation in body
      val result = body
      result match {
        case asyncResult: Future[_] =>
          implicit val ec: ExecutionContext = directExecutionContext

          // Cleanup after completion of the future.
          val asyncResultWithCleanup = asyncResult
            .map(r => {
              onSuccess()
              r
            })
            .thereafter(_ => doFinally())

          // Switch off cleanup in finally block, as that would be performed too early
          isAsync = true

          // Return future that completes after termination of body and cleanup
          asyncResultWithCleanup.asInstanceOf[T]

        case _: EitherT[_, _, _] | _: OptionT[_, _] =>
          throw new IllegalArgumentException(
            "Suppression for EitherT and OptionT is currently not supported. Please unwrap the result by calling `_.value`."
          )

        // Unable to support this, because we can't access the first type parameter.
        // Therefore, we don't know whether the suppression needs to be asynchronous.

        case syncResult =>
          onSuccess()
          syncResult
      }
    } finally {
      if (!isAsync)
        // Cleanup, if body fails synchronously, i.e.
        // - body is not of type Future, or
        // - body is of type Future, but it terminates abruptly without constructing a future
        doFinally()
    }
  }

  private def beginSuppress(rule: SuppressionRule): () => Unit = {
    withClue("Trying to suppress warnings/errors several times") {
      // Nested usages are not supported, because we clear the message queue when the suppression begins.
      // So a second call of this method would purge the messages collected by previous calls.

      val previousRule = activeSuppressionRule.getAndUpdate { (previous: SuppressionRule) =>
        if (previous == SuppressionRule.NoSuppression) rule else previous
      }
      previousRule shouldBe SuppressionRule.NoSuppression
    }

    recordedLogEntries.clear()

    () => activeSuppressionRule.set(SuppressionRule.NoSuppression)
  }

  def assertSingleErrorLogEntry[A](
      within: => A,
      expectedMsg: String,
      expectedMDC: Map[String, String],
      expectedThrowable: Option[Throwable],
  ): A =
    assertLogs(
      within,
      logEntry => {

        val cp = new Checkpoint

        cp {
          logEntry.errorMessage shouldBe expectedMsg
        }

        val mdc = logEntry.mdc.map { case (k, v) =>
          (k, v.replaceAll("\\.scala:\\d+", ".scala:<line-number>"))
        }
        cp {
          forEvery(expectedMDC)(entry => mdc should contain(entry))
        }

        cp {
          logEntry.throwable.map(_.toString) shouldBe expectedThrowable.map(_.toString)
        }
        cp.reportAll()
        succeed
      },
    )
}

object SuppressingLogger {
  def apply(
      testClass: Class[_],
      pollTimeout: FiniteDuration = 1.second,
      skipLogEntry: LogEntry => Boolean = defaultSkipLogEntry,
  ): SuppressingLogger =
    new SuppressingLogger(
      NamedLoggerFactory.unnamedKey("test", testClass.getSimpleName),
      pollTimeout,
      skipLogEntry = skipLogEntry,
    )

  final case class LogEntryCriterion(level: Level, loggerNamePrefix: String, pattern: Regex) {
    def matches(logEntry: LogEntry): Boolean =
      logEntry.level == level &&
        logEntry.loggerName.startsWith(loggerNamePrefix) &&
        (logEntry.message match {
          case pattern(_*) => true
          case _ => false
        })
  }

  def assertThatLogDoesntContainUnexpected(
      expectedProblems: List[String]
  )(logs: Seq[LogEntry]): Assertion = {
    forEvery(logs) { x =>
      assert(
        expectedProblems.exists(msg => x.toString.contains(msg)),
        s"line $x contained unexpected problem",
      )
    }
  }

  /** Lists criteria for log entries that are skipped during suppression in all test cases */
  val skippedLogEntries: Seq[LogEntryCriterion] = Seq(
    LogEntryCriterion(
      WARN,
      classOf[ExecutionContextMonitor].getName,
      ("""(?s)Task runner .* is .* overloaded.*""").r,
    )
  )

  def defaultSkipLogEntry(logEntry: LogEntry): Boolean =
    skippedLogEntries.exists(_.matches(logEntry))

  sealed trait LogEntryOptionality extends Product with Serializable
  object LogEntryOptionality {
    case object Required extends LogEntryOptionality

    /** Once or not at all */
    case object Optional extends LogEntryOptionality

    /** Can be there many times but doesn't have to be */
    case object OptionalMany extends LogEntryOptionality
  }
}
