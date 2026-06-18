// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.aws.tracing

import com.digitalasset.canton.crypto.kms.aws.audit.AwsRequestResponseLogger.traceContextExecutionAttribute
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.tracing.{Spanning, TraceContext, TracerProvider}
import io.opentelemetry.api.trace.Span
import software.amazon.awssdk.core.interceptor.{
  Context,
  ExecutionAttribute,
  ExecutionAttributes,
  ExecutionInterceptor,
  SdkExecutionAttribute,
}

import scala.compat.java8.OptionConverters.RichOptionalGeneric

import AwsTraceContextInterceptor.{otelSpanExecutionAttribute, withTraceContext}

/** Starts a new trace span before each request and end it when receiving the response
  */
class AwsTraceContextInterceptor(
    override val loggerFactory: NamedLoggerFactory,
    tracerProvider: TracerProvider,
) extends ExecutionInterceptor
    with NamedLogging
    with Spanning {
  override def beforeExecution(
      context: Context.BeforeExecution,
      executionAttributes: ExecutionAttributes,
  ): Unit =
    withTraceContext(executionAttributes, logger) { tc =>
      val spanName = Option(executionAttributes.getAttribute(SdkExecutionAttribute.OPERATION_NAME))
        .getOrElse("Aws-Kms-Operation")
      val span = tracerProvider.tracer
        .spanBuilder(spanName)
        .setParent(tc.context)
        .startSpan()
      executionAttributes.putAttribute(otelSpanExecutionAttribute, span)
      ()
    }

  override def afterExecution(
      context: Context.AfterExecution,
      executionAttributes: ExecutionAttributes,
  ): Unit =
    Option(executionAttributes.getAttribute(otelSpanExecutionAttribute)).foreach(_.end())
}

object AwsTraceContextInterceptor {
  private[aws] val otelSpanExecutionAttribute =
    new ExecutionAttribute[Span]("canton-otel-span")

  // Extract the canton trace context from the attributes, should be set with ExtendedAwsRequestBuilder.withTraceContext
  private[aws] def withTraceContext[A](
      executionAttributes: ExecutionAttributes,
      logger: TracedLogger,
  )(f: TraceContext => A): A = {
    val tc =
      executionAttributes.getOptionalAttribute(traceContextExecutionAttribute).asScala.getOrElse {
        val emptyTc = TraceContext.empty
        logger.info(
          "Missing canton trace context, please make sure that all Aws request builders set the trace context execution attribute. See c.d.c.crypto.kms.AwsKms.traceContextExecutionAttribute"
        )(emptyTc)
        TraceContext.empty
      }
    f(tc)
  }
}
