// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import org.mockito.ArgumentMatchersSugar
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.event.{EventConstants, Level}
import org.slf4j.{Logger, Marker}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
final class ContextualizedLoggerSpec
    extends FlatSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  behavior of "ContextualizedLogger"

  it should "leave the logs unchanged if the logging context is empty" in
    withEmptyContext { logger => implicit logCtx =>
      logger.info("foobar")
      verify(logger.withoutContext).info("foobar")
    }

  it should "decorate the logs with the provided context" in
    withContext("id" -> "foobar") { logger => implicit logCtx =>
      logger.info("a")
      val m = logger.withoutContext
      verify(m).info(eqTo("a (context: {})"), toStringEqTo[AnyRef]("{id=foobar}"))
    }

  it should "pass the context via the markers if a throwable is provided" in
    withContext("id" -> "foo") { logger => implicit logCtx =>
      logger.error("a", new IllegalArgumentException("quux"))
      verify(logger.withoutContext).error(
        toStringEqTo[Marker]("{id=foo}"),
        eqTo("a (context: {id=foo})"),
        withMessage[IllegalArgumentException]("quux"))
    }

  def thisThrows(): String = throw new RuntimeException("failed on purpose")

  it should "construct log entries lazily based on the required level" in
    withEmptyContext { logger => implicit logCtx =>
      noException should be thrownBy { logger.debug(s"${thisThrows()}") }
      verify(logger.withoutContext, times(0)).debug(any[String])
    }

  it should "always pick the context in the most specific scope" in
    withContext("i1" -> "x") { logger => implicit logCtx =>
      logger.info("a")
      LoggingContext.withEnrichedLoggingContext("i2" -> "y") { implicit logCtx =>
        logger.info("b")
      }
      logger.info("c")
      val m = logger.withoutContext
      verify(m).info(eqTo("a (context: {})"), toStringEqTo[AnyRef]("{i1=x}"))
      verify(m).info(eqTo("b (context: {})"), toStringEqTo[AnyRef]("{i1=x, i2=y}"))
      verify(m).info(eqTo("c (context: {})"), toStringEqTo[AnyRef]("{i1=x}"))
    }

  it should "override with values provided in a more specific scope" in
    withContext("id" -> "foobar") { logger => implicit logCtx =>
      logger.info("a")
      LoggingContext.withEnrichedLoggingContext("id" -> "quux") { implicit logCtx =>
        logger.info("b")
      }
      logger.info("c")
      val m = logger.withoutContext
      verify(m).info(eqTo("a (context: {})"), toStringEqTo[AnyRef]("{id=foobar}"))
      verify(m).info(eqTo("b (context: {})"), toStringEqTo[AnyRef]("{id=quux}"))
      verify(m).info(eqTo("c (context: {})"), toStringEqTo[AnyRef]("{id=foobar}"))
    }

  it should "pick the expected context also when executing in a future" in
    withContext("id" -> "future") { logger => implicit logCtx =>
      import scala.concurrent.ExecutionContext.Implicits.global
      import scala.concurrent.duration.DurationInt
      import scala.concurrent.{Await, Future}

      val f1 = Future { logger.info("a") }
      LoggingContext.withEnrichedLoggingContext("id" -> "next") { implicit logCtx =>
        val f2 = Future { logger.info("b") }
        Await.result(Future.sequence(Seq(f1, f2)), 10.seconds)
      }
      val m = logger.withoutContext
      verify(m).info(eqTo("a (context: {})"), toStringEqTo[AnyRef]("{id=future}"))
      verify(m).info(eqTo("b (context: {})"), toStringEqTo[AnyRef]("{id=next}"))
    }

  it should "drop the context if new context is provided at a more specific scope" in
    withContext("id" -> "foobar") { logger => implicit logCtx =>
      logger.info("a")
      LoggingContext.newLoggingContext { implicit logCtx =>
        logger.info("b")
      }
      logger.info("d")
      val m = logger.withoutContext
      verify(m).info(eqTo("a (context: {})"), toStringEqTo[AnyRef]("{id=foobar}"))
      verify(m).info("b")
      verify(m).info(eqTo("d (context: {})"), toStringEqTo[AnyRef]("{id=foobar}"))
    }

  it should "allow the user to use the underlying logger, foregoing context" in
    withContext("id" -> "foobar") { logger => _ =>
      logger.withoutContext.info("foobar")
      verify(logger.withoutContext).info("foobar")
    }

  it should "allows users to pick and choose between the contextualized logger and the underlying one" in
    withContext("id" -> "foobar") { logger => implicit logCtx =>
      logger.withoutContext.info("a")
      logger.info("b")
      val m = logger.withoutContext
      verify(m).info("a")
      verify(m).info(eqTo("b (context: {})"), toStringEqTo[AnyRef]("{id=foobar}"))
    }

  def withEmptyContext(f: ContextualizedLogger => LoggingContext => Unit): Unit =
    LoggingContext.newLoggingContext(f(ContextualizedLogger.createFor(mockLogger(Level.INFO))))

  def withContext(kv: (String, String))(f: ContextualizedLogger => LoggingContext => Unit): Unit =
    LoggingContext.newLoggingContext(kv)(f(ContextualizedLogger.createFor(mockLogger(Level.INFO))))

  def mockLogger(level: Level): Logger = {
    val mocked = mock[Logger]
    when(mocked.isTraceEnabled()).thenReturn(level.toInt <= EventConstants.TRACE_INT)
    when(mocked.isDebugEnabled()).thenReturn(level.toInt <= EventConstants.DEBUG_INT)
    when(mocked.isInfoEnabled()).thenReturn(level.toInt <= EventConstants.INFO_INT)
    when(mocked.isWarnEnabled()).thenReturn(level.toInt <= EventConstants.WARN_INT)
    when(mocked.isErrorEnabled()).thenReturn(level.toInt <= EventConstants.ERROR_INT)
    mocked
  }

  def toStringEqTo[T](s: String): T =
    argThat[T]((_: T).toString == s)

  def withMessage[T <: Throwable](s: String): Throwable =
    argThat[T]((_: T).getMessage == s)

}
