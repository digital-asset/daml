// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.sequencing.AsyncResult
import com.digitalasset.canton.tracing.Spanning.{SpanEndingExecutionContext, SpanWrapper}
import com.digitalasset.canton.util.{Checked, CheckedT}
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.{Span, StatusCode, Tracer}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

trait Spanning {
  protected def withSpanFromGrpcContext[A](description: String)(
      f: TraceContext => SpanWrapper => A
  )(implicit tracer: Tracer): A = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    withSpan(description)(f)(traceContext, tracer)
  }

  protected def withNewTrace[A](description: String)(f: TraceContext => SpanWrapper => A)(implicit
      tracer: Tracer
  ): A =
    withSpan(description)(f)(TraceContext.empty, tracer)

  protected def withSpan[A](
      description: String
  )(f: TraceContext => SpanWrapper => A)(implicit traceContext: TraceContext, tracer: Tracer): A = {
    val currentSpan = startSpan(description)

    def closeSpan(value: Any): Unit = value match {
      case future: Future[_] =>
        closeOnComplete(future)
      case eitherT: EitherT[_, _, _] =>
        closeSpan(eitherT.value)
      case Right(x) => closeSpan(x) // Look into the result of an EitherT
      case optionT: OptionT[_, _] =>
        closeSpan(optionT.value)
      case Some(x) => closeSpan(x) // Look into the result of an OptionT
      case checkedT: CheckedT[_, _, _, _] =>
        closeSpan(checkedT.value)
      case Checked.Result(_, x) => closeSpan(x) // Look into the result of a CheckedT
      case unlessShutdown: UnlessShutdown.Outcome[_] =>
        // Look into the result of a FutureUnlessShutdown
        closeSpan(unlessShutdown.result)
      case asyncResult: AsyncResult =>
        closeSpan(asyncResult.unwrap)
      case _ =>
        currentSpan.end()
    }

    def closeOnComplete(f: Future[_]): Unit =
      f.onComplete {
        case Success(x) =>
          closeSpan(x)
        case Failure(exception) =>
          recordException(exception).discard
          currentSpan.end()
      }(SpanEndingExecutionContext)

    def recordException(exception: Throwable) = {
      currentSpan.recordException(exception)
      currentSpan.setStatus(StatusCode.ERROR, "Operation ended with error")
    }

    val result: A =
      try {
        val childContext = TraceContext(currentSpan.storeInContext(traceContext.context))
        f(childContext)(new SpanWrapper(currentSpan))
      } catch {
        case NonFatal(exception) =>
          recordException(exception).discard
          currentSpan.end()
          throw exception
      }
    closeSpan(result)
    result
  }

  private def startSpan(
      description: String
  )(implicit parentTraceContext: TraceContext, tracer: Tracer): Span = {
    val currentSpan = tracer
      .spanBuilder(description)
      .setParent(parentTraceContext.context)
      .startSpan()
    currentSpan.setAttribute("canton.class", getClass.getName)
    currentSpan
  }
}

object Spanning {
  // this execution context is solely used to end spans, which is a non-blocking operation that
  // does not throw any expected exceptions
  private object SpanEndingExecutionContext extends ExecutionContext {
    override def execute(r: Runnable): Unit = r.run()
    override def reportFailure(t: Throwable): Unit =
      throw new IllegalStateException("unexpected error ending span", t)
  }

  class SpanWrapper(span: Span) {
    def addEvent(name: String, attributes: Map[String, String] = Map()): Unit = {
      val _ = span.addEvent(name, mapToAttributes(attributes))
    }
    def setAttribute(key: String, value: String): Unit = {
      val _ = span.setAttribute(s"canton.$key", value)
    }
    def recordException(exception: Throwable, attributes: Map[String, String] = Map()): Unit = {
      val _ = span.recordException(exception, mapToAttributes(attributes))
    }

    def getSpanId: String = span.getSpanContext.getSpanId
  }
  private def mapToAttributes(map: Map[String, String]): Attributes =
    map
      .foldRight(Attributes.builder()) { case ((key, value), builder) =>
        builder.put(s"canton.$key", value)
      }
      .build()
}
