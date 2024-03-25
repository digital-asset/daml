// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.typesafe.scalalogging.Logger
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j
import org.slf4j.event.Level

import scala.collection.immutable.ListMap
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}

class SuppressingLoggerTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  "suppress" should {

    "works normally without suppressions" in new LoggingTester {
      logger.error("Test")

      verify(underlyingLogger).error("Test")
    }

    "suppress intended log messages" in new LoggingTester {
      loggerFactory.assertLogs(
        logger.error("Test"),
        _.errorMessage shouldBe "Test",
      )

      verify(underlyingLogger, never).error(any[String])
      verify(underlyingLogger).info("Suppressed ERROR: Test")

      // check it's no longer suppressed
      logger.error("TestAgain")

      verify(underlyingLogger).error("TestAgain")
      verify(underlyingLogger, atMost(1)).info(any[String])
      loggerFactory.recordedLogEntries shouldBe empty
    }

    "propagate exceptions" in new LoggingTester {
      val ex = new RuntimeException("Test exception")

      // An exception gets propagated
      the[RuntimeException] thrownBy loggerFactory.assertLogs(
        {
          logger.error("Test")
          throw ex
        },
        _ => fail("Log messages should not be checked on exception."),
      ) shouldBe ex

      // Errors get suppressed anyway
      verify(underlyingLogger, never).error(any[String])
      verify(underlyingLogger).info("Suppressed ERROR: Test")

      // ... and can be checked
      loggerFactory.recordedLogEntries.loneElement.errorMessage shouldBe "Test"

      // check it's no longer suppressed
      logger.error("TestAgain")

      verify(underlyingLogger).error("TestAgain")
      verify(underlyingLogger, atMost(1)).info(any[String])

      // Errors will be deleted before suppressing again.
      loggerFactory.assertLoggedWarningsAndErrorsSeq({}, _ shouldBe empty)
    }

    "point out first failure and remaining errors" in new LoggingTester() {
      val ex: TestFailedException = the[TestFailedException] thrownBy loggerFactory.assertLogs(
        {
          logger.error("Test1")
          logger.error("foo")
          logger.error("Test3")
          logger.error("Test4")
        },
        _.errorMessage shouldBe "Test1",
        _.errorMessage shouldBe "Test2",
      )

      ex.getMessage() should startWith regex
        // The ( ) here are pointless groups in the regex that ensure that the trailing whitespace is not removed automatically by the setting in editorconfig
        """forEvery failed, because:( )
          |  at index 1, "\[foo\]" was not equal to "\[Test2\]"
          |(  )
          |  Remaining log entries:
          |  	## ERROR c\.d\.c\.l\.SuppressingLoggerTest.*:TestLogger - Test3
          |  	## ERROR c\.d\.c\.l\.SuppressingLoggerTest.*:TestLogger - Test4
          |""".stripMargin
    }

    "fail gracefully on unexpected errors" in new LoggingTester {
      val ex: TestFailedException = the[TestFailedException] thrownBy loggerFactory.assertLogs(
        {
          logger.error("Test1")
          logger.error("Test2")
        },
        _.errorMessage shouldBe "Test1",
      )

      ex.getMessage() should fullyMatch regex
        """Found unexpected log messages:
          |	## ERROR c\.d\.c\.l\.SuppressingLoggerTest.*:TestLogger - Test2
          |""".stripMargin
    }

    "suppress intended log messages during asynchronous operation" in new LoggingTester {
      val promise: Promise[Unit] = Promise[Unit]()

      val fut = loggerFactory.assertLogs(promise.future, _.errorMessage shouldBe "Test")

      logger.error("Test")

      promise.success(())
      fut.futureValue

      verify(underlyingLogger, never).error(any[String])
      verify(underlyingLogger).info("Suppressed ERROR: Test")

      // check it's no longer suppressed
      logger.error("TestAgain")

      verify(underlyingLogger).error("TestAgain")
      verify(underlyingLogger, atMost(1)).info(any[String])
      loggerFactory.recordedLogEntries shouldBe empty
    }

    "propagate async exceptions" in new LoggingTester {
      val ex: RuntimeException = new RuntimeException("Test exception")
      val promise: Promise[Unit] = Promise[Unit]()

      // An exception gets propagated0
      val future: Future[Unit] =
        loggerFactory.assertLogs(
          promise.future,
          _ => fail("Log messages should not be checked on exception."),
        )

      logger.error("Test")
      promise.failure(ex)

      future.failed.futureValue shouldBe ex

      // Errors get suppressed anyway
      verify(underlyingLogger, never).error(any[String])
      verify(underlyingLogger).info("Suppressed ERROR: Test")

      // ... and can be checked
      loggerFactory.recordedLogEntries.loneElement.errorMessage shouldBe "Test"

      // check it's no longer suppressed
      logger.error("TestAgain")

      verify(underlyingLogger).error("TestAgain")
      verify(underlyingLogger, atMost(1)).info(any[String])

      // Errors will be deleted before suppressing again.
      loggerFactory.assertLoggedWarningsAndErrorsSeq({}, _ shouldBe empty)
    }

    "save messages only from the last suppression block" in new LoggingTester {
      loggerFactory.assertLogs(
        logger.error("First"),
        _.errorMessage shouldBe "First",
      )

      loggerFactory.assertLogs(
        logger.error("Second"),
        _.errorMessage shouldBe "Second",
      )
    }

    "nicely format messages" in new LoggingTester {
      loggerFactory.assertLogs(
        logger.error(s"abc ${2 + 3} def ${1 + 2}"),
        _.errorMessage shouldBe "abc 5 def 3",
      )
    }

    "ignore order if told so" in new LoggingTester {
      loggerFactory.assertLogsUnordered(
        {
          logger.error("First")
          logger.error("Second")
          logger.error("Third")
        },
        _.errorMessage shouldBe "Third",
        _.errorMessage shouldBe "First",
        _.errorMessage shouldBe "Second",
      )
    }

    "match assertions sequentially" in new LoggingTester {
      loggerFactory.assertLogsUnordered(
        {
          logger.error("Message")
          logger.error("LongMessage")
        },
        _.errorMessage should include("LongMessage"),
        _.errorMessage should include("Message"),
      )
    }

    "match the repeated assertions repeatedly" in new LoggingTester {
      loggerFactory.assertLogsUnordered(
        {
          logger.error("First message")
          logger.error("Second message")
        },
        _.errorMessage should include("message"),
        _.errorMessage should include("message"),
      )
    }

    "fail gracefully on unmatched unordered messages" in new LoggingTester {
      val ex: TestFailedException =
        the[TestFailedException] thrownBy loggerFactory.assertLogsUnordered(
          {
            logger.error("Second message")
          },
          _.errorMessage shouldBe "First message",
          _.errorMessage shouldBe "Second message",
        )

      ex.getMessage should fullyMatch regex
        """No log message has matched the assertions with index 0.
          |
          |Matched log messages:
          |1:	## ERROR c.d.c.l.SuppressingLoggerTest.*:TestLogger - Second message
          |""".stripMargin
    }

    "fail gracefully on unexpected unordered messages" in new LoggingTester {
      val ex: TestFailedException =
        the[TestFailedException] thrownBy loggerFactory.assertLogsUnordered(
          {
            logger.error("First message")
            logger.error("Second message")
          },
          _.errorMessage shouldBe "Second message",
        )

      ex.getMessage should fullyMatch regex
        """Found unexpected log messages:
          |	## ERROR c.d.c.l.SuppressingLoggerTest.*:TestLogger - First message
          |
          |Matched log messages:
          |0:	## ERROR c.d.c.l.SuppressingLoggerTest.*:TestLogger - Second message
          |""".stripMargin
    }

    "fail gracefully on unmatched and unexpected unordered messages" in new LoggingTester {
      val ex: TestFailedException =
        the[TestFailedException] thrownBy loggerFactory.assertLogsUnordered(
          {
            logger.error("Unmatched message")
            logger.error("Second message")
            logger.error("Third message")
          },
          _.errorMessage shouldBe "First message",
          _.errorMessage shouldBe "Second message",
        )

      ex.getMessage should fullyMatch regex
        """Found unexpected log messages:
          |	## ERROR c.d.c.l.SuppressingLoggerTest.*:TestLogger - Unmatched message
          |	## ERROR c.d.c.l.SuppressingLoggerTest.*:TestLogger - Third message
          |
          |No log message has matched the assertions with index 0.
          |
          |Matched log messages:
          |1:	## ERROR c.d.c.l.SuppressingLoggerTest.*:TestLogger - Second message
          |""".stripMargin
    }

    "ignore missing optional unordered errors" in new LoggingTester {
      import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality.*
      loggerFactory.assertLogsUnorderedOptional(
        {
          logger.error("First")
          logger.error("Second")
          logger.error("Third")
        },
        Optional -> (_.errorMessage shouldBe "Fourth"),
        Required -> (_.errorMessage shouldBe "Third"),
        Optional -> (_.errorMessage shouldBe "First"),
        Required -> (_.errorMessage shouldBe "Second"),
      )
    }

    "skip errors that are to be skipped" in new LoggingTester {
      override def skipLogEntry(logEntry: LogEntry): Boolean = {
        logEntry.level == slf4j.event.Level.ERROR &&
        logEntry.loggerName.startsWith(classOf[SuppressingLogger].getName) &&
        logEntry.message == "message"
      }

      loggerFactory.assertLogs(
        {
          logger.error("message")
          logger.error("another message")
          logger.error("message")
          logger.error("yet another message")
        },
        _.errorMessage shouldBe "another message",
        _.errorMessage shouldBe "yet another message",
      )
      verify(underlyingLogger, times(2)).info(s"Suppressed ERROR: message")
    }

    "check sequence of log entries" in new LoggingTester {
      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        {
          logger.error("Test1")
          logger.error("Test2")
          logger.error("Test3")
          logger.error("Test4")
        },
        entries =>
          forAtLeast(1, entries)(
            _.errorMessage shouldBe ("Test2")
          ),
      )
    }

    "point out failed assertion against sequence of log entries" in new LoggingTester {
      the[TestFailedException] thrownBy loggerFactory.assertLogsSeq(
        SuppressionRule.LevelAndAbove(Level.WARN)
      )(
        {
          logger.error("Test1")
          logger.error("Test2")
          logger.error("Test3")
          logger.error("Test4")
        },
        entries =>
          forEvery(entries)(
            _.errorMessage shouldBe "Test2"
          ),
      )
    }

    "check sequence of log entries eventually" in new LoggingTester {
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        {
          logger.error("Test1")
          logger.error("Test2")
          logger.error("Test3")
          logger.error("Test4")
        },
        entries =>
          forAtLeast(1, entries)(
            _.errorMessage shouldBe ("Test2")
          ),
      )
      FutureUtil.doNotAwait(
        Future {
          Threading.sleep(1.seconds.toMillis)
          logger.error("Test3")
          logger.error("Test4")
        },
        "unexpected error",
      )

      loggerFactory.assertEventuallyLogsSeq(
        SuppressionRule.LevelAndAbove(Level.WARN)
      )(
        {},
        entries =>
          forAtLeast(1, entries)(
            _.errorMessage shouldBe "Test4"
          ),
      )

      val async = Future {
        logger.error("Test1")
        logger.error("Test2")
        Threading.sleep(1.seconds.toMillis)
        logger.error("Test3")
        logger.error("Test4")
      }

      loggerFactory
        .assertEventuallyLogsSeq(
          SuppressionRule.LevelAndAbove(Level.WARN)
        )(
          async,
          entries =>
            forAtLeast(1, entries)(
              _.errorMessage shouldBe "Test4"
            ),
        )
        .futureValue
    }

    "point out failed assertion against sequence of log entries eventually" in new LoggingTester {
      the[TestFailedException] thrownBy loggerFactory.assertEventuallyLogsSeq(
        SuppressionRule.LevelAndAbove(Level.WARN)
      )(
        {
          logger.error("Test1")
          logger.error("Test2")
          logger.error("Test3")
          logger.error("Test4")
        },
        entries =>
          forEvery(entries)(
            _.errorMessage shouldBe "Test2"
          ),
        timeUntilSuccess = 1.seconds,
      )

      the[TestFailedException] thrownBy loggerFactory
        .assertEventuallyLogsSeq(
          SuppressionRule.LevelAndAbove(Level.WARN)
        )(
          Future {
            Threading.sleep(1.seconds.toMillis)
            logger.error("Test1")
            logger.error("Test2")
            logger.error("Test3")
            logger.error("Test4")
          },
          entries =>
            forAtLeast(1, entries)(
              _.errorMessage shouldBe "Never happen"
            ),
          timeUntilSuccess = 2.seconds,
        )
        .futureValue
    }
  }

  "Throwable.addSuppressed" should {
    "also log the suppressed exception" in new LoggingTester {
      val ex1 = new RuntimeException("ONE")
      val ex2 = new RuntimeException("TWO")
      ex1.addSuppressed(ex2)

      loggerFactory.assertLogs(
        logger.error("Test", ex1),
        entry => {
          entry.errorMessage should include("Test")
          entry.throwable.value.getMessage shouldBe "ONE"
          entry.throwable.value.getSuppressed should contain(ex2)
          ErrorUtil.messageWithStacktrace(entry.throwable.value) should include("TWO")
        },
      )
    }
  }

  class LoggingTester extends NamedLogging {
    val underlyingNamedLogger = new TestNamedLogger
    def skipLogEntry(_logEntry: LogEntry): Boolean = false
    val loggerFactory: SuppressingLogger =
      new SuppressingLogger(underlyingNamedLogger, pollTimeout = 10.millis, skipLogEntry)
    val underlyingLogger: slf4j.Logger = underlyingNamedLogger.logger
  }

  class TestNamedLogger extends NamedLoggerFactory {
    val logger: slf4j.Logger = mock[slf4j.Logger]
    when(logger.isErrorEnabled).thenReturn(true)
    override val name: String = "TestLogger"
    override val properties: ListMap[String, String] = ListMap.empty
    override def appendUnnamedKey(key: String, value: String): NamedLoggerFactory = this
    override def append(key: String, value: String): NamedLoggerFactory = this
    override private[logging] def getLogger(fullName: String): Logger = Logger(logger)
  }
}
