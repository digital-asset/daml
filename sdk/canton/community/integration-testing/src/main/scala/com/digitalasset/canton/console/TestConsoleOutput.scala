// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.ErrorUtil
import org.scalactic.source
import org.scalatest.Assertion
import org.scalatest.Inspectors.*
import org.scalatest.matchers.should.Matchers.*

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Future, TimeUnit}
import scala.concurrent.blocking
import scala.jdk.CollectionConverters.*

/** Implementation of [[com.digitalasset.canton.console.ConsoleOutput]] for test purposes.
  * By default, it logs messages as errors to fail the build on unexpected output.
  * Alternatively, messages can be recorded and checked.
  */
class TestConsoleOutput(override val loggerFactory: NamedLoggerFactory)
    extends ConsoleOutput
    with NamedLogging
    with NoTracing {

  private val messageQueue: java.util.concurrent.BlockingQueue[String] =
    new java.util.concurrent.LinkedBlockingQueue[String]()

  private val recording: AtomicBoolean = new AtomicBoolean(false)

  def startRecording(): Unit = {
    ErrorUtil.requireArgument(!recording.getAndSet(true), "already recording!")
    messageQueue.clear()
  }

  def stopRecording(): Seq[String] = {
    ErrorUtil.requireArgument(recording.getAndSet(false), "have not been recording!")
    val ret = messageQueue.asScala.toList
    messageQueue.clear()
    ret
  }

  override def info(message: String): Unit = {
    if (recording.get())
      messageQueue.add(message)
    else
      // Please use recordMessages if you see this error in tests.
      logger.error(s"Unexpected console output: $message")
  }

  /** Executes a piece of code, records the console output created by that code and checks whether the sequence
    * of emitted messages meets a sequence of assertions.
    *
    * @return the result of operation
    * @throws java.lang.IllegalArgumentException if recording has already been enabled. I.e., no nested usage is supported.
    * @throws java.lang.UnsupportedOperationException if `T` is `Future[_]`, `EitherT` or `OptionT`.
    */
  def assertConsoleOutput[T](operation: => T, assertions: (String => Assertion)*)(implicit
      pos: source.Position
  ): T = {
    // We need to prevent nested usage, because we are clearing messageQueue.
    blocking(this.synchronized {
      require(!recording.get(), "Nested use of Unable to record console output messages.")
      recording.set(true)
    })

    messageQueue.clear()

    try {
      // Perform operation
      val result = operation

      result match {
        case _: Future[_] | _: EitherT[_, _, _] | _: OptionT[_, _] =>
          throw new UnsupportedOperationException(
            "Recording messages is not supported for possibly asynchronous operations."
          )
        case _ => // continue checking
      }

      // Check recorded messages
      // Check that every assertion succeeds on the corresponding log entry
      forEvery(assertions) { assertion =>
        Option(messageQueue.poll(1, TimeUnit.SECONDS)) match {
          case Some(logEntry) => assertion(logEntry)
          case None => fail(s"Missing console message.")
        }
      }

      // Check that there are no unexpected log entries
      withClue("Found unexpected console messages:") {
        messageQueue.asScala shouldBe empty
      }
      // Return result in case of success
      result
    } finally {
      recording.set(false)
    }
  }
}
