// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.console.CommandErrors.GenericCommandError
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.logging.{NamedEventCapturingLogger, SuppressingLogger}
import com.digitalasset.canton.metrics.OnDemandMetricsReader.NoOpOnDemandMetricsReader$
import com.digitalasset.canton.telemetry.ConfiguredOpenTelemetry
import com.digitalasset.canton.tracing.TracerProvider
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.trace.SdkTracerProvider
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import scala.util.control.NonFatal

@SuppressWarnings(Array("org.wartremover.warts.Null"))
class ConsoleEnvironmentTest extends AnyWordSpec with BaseTest {

  // Prevent accidental use of the logger factory from base test
  override val loggerFactory: SuppressingLogger = null

  private class Fixture {

    // Intercept log events
    val capturingLoggerFactory: NamedEventCapturingLogger = new NamedEventCapturingLogger(
      getClass.getSimpleName
    )

    // Intercepts the console output
    val testConsoleOutput: TestConsoleOutput = new TestConsoleOutput(capturingLoggerFactory)

    // Setup environment to inject capturing loggerFactory
    val environment = mock[Environment]
    when(environment.loggerFactory).thenReturn(capturingLoggerFactory)
    when(environment.config).thenReturn(CantonConfig())
    when(environment.tracerProvider).thenReturn(mock[TracerProvider])
    when(environment.configuredOpenTelemetry).thenReturn(
      ConfiguredOpenTelemetry(
        OpenTelemetrySdk.builder().build(),
        SdkTracerProvider.builder(),
        NoOpOnDemandMetricsReader$,
      )
    )

    // The ConsoleEnvironment to be tested
    val consoleEnvironment: ConsoleEnvironment = new ConsoleEnvironment(
      environment,
      consoleOutput = testConsoleOutput,
    )
  }
  private lazy val fixture = new Fixture()

  "The console environment" when {
    "executing a successful command" must {
      "continue processing" in {
        fixture.consoleEnvironment.run(CommandSuccessful(42)) shouldBe 42
      }

      "log nothing" in {
        assertEmptyLog()
      }
    }

    "executing a command with a null result" must {
      val missingResultMessage = "Console command has returned 'null' as result."

      "abort processing with an internal error" in {
        fixture.capturingLoggerFactory.eventQueue.clear()
        a[CantonInternalError] should be thrownBy fixture.consoleEnvironment.run(null)
      }

      "log an error" in {
        assertFirstLogMessage(missingResultMessage, Level.ERROR)
        assertEmptyLog()
      }
    }

    "executing a failed command" must {
      val failureMessage = "Some problem occurred."

      "abort processing with a command failure" in {
        fixture.capturingLoggerFactory.eventQueue.clear()
        a[CommandFailure] should be thrownBy fixture.consoleEnvironment.run(
          GenericCommandError(failureMessage)
        )
      }

      "log the message with error level" in {
        assertFirstLogMessage(failureMessage, Level.ERROR)
        assertEmptyLog()
      }
    }

    val throwables = Map(
      "runtime exception" -> new RuntimeException(),
      "fatal error" -> new VirtualMachineError() {},
    )

    forEvery(throwables) { case (name, injectedThrowable) =>
      s"observing a $name" must {
        "abort processing with an internal error" in {
          fixture.capturingLoggerFactory.eventQueue.clear()
          injectedThrowable match {
            case NonFatal(_) =>
              a[CantonInternalError] should be thrownBy fixture.consoleEnvironment.run(
                throw injectedThrowable
              )
            case _ =>
              the[Throwable] thrownBy fixture.consoleEnvironment.run(
                throw injectedThrowable
              ) shouldBe injectedThrowable
          }
        }

        "log the exception as an error" in {
          assertFirstLogMessage(
            "An internal error has occurred while running a console command.",
            Level.ERROR,
            injectedThrowable,
          )
          assertEmptyLog()
        }
      }
    }
  }

  def assertFirstLogMessage(
      expectedMessage: String,
      expectedLevel: Level,
      throwable: Throwable = null,
  ): Assertion = {
    withClue(s"No event has been logged. Expected: $expectedMessage") {
      fixture.capturingLoggerFactory.eventQueue should not be empty
    }

    val event = fixture.capturingLoggerFactory.eventQueue.poll()
    event should not be null
    event.message should include(expectedMessage)
    event.throwable shouldEqual Option(throwable)
    event.level shouldEqual expectedLevel
  }

  def assertEmptyLog(): Assertion =
    withClue("Unexpected events have been logged.") {
      fixture.capturingLoggerFactory.eventQueue shouldBe empty
    }
}
