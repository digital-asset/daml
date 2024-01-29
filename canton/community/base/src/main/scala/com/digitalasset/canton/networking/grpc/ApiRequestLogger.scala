// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.ShowUtil.*
import com.google.common.annotations.VisibleForTesting
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.Status.Code.*
import io.grpc.*

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try

/** Server side interceptor that logs incoming and outgoing traffic.
  *
  * @param config Configuration to tailor the output
  */
class ApiRequestLogger(
    override protected val loggerFactory: NamedLoggerFactory,
    config: ApiLoggingConfig,
) extends ApiRequestLoggerBase(loggerFactory, config)
    with ServerInterceptor
    with NamedLogging {

  @VisibleForTesting
  private[networking] val cancelled: AtomicBoolean = new AtomicBoolean(false)

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    val requestTraceContext: TraceContext = inferRequestTraceContext

    val sender = Option(call.getAttributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString)
      .getOrElse("unknown sender")
    val method = call.getMethodDescriptor.getFullMethodName

    def createLogMessage(message: String): String =
      show"Request ${method.readableLoggerName(config.maxMethodLength)} by ${sender.unquoted}: ${message.unquoted}"

    logger.trace(createLogMessage(s"received headers ${stringOfMetadata(headers)}"))(
      requestTraceContext
    )

    val loggingServerCall = new LoggingServerCall(call, createLogMessage, requestTraceContext)
    val serverCallListener = next.startCall(loggingServerCall, headers)
    new LoggingServerCallListener(serverCallListener, createLogMessage, requestTraceContext)
  }

  /** Intercepts events sent by the client.
    */
  class LoggingServerCallListener[ReqT, RespT](
      delegate: ServerCall.Listener[ReqT],
      createLogMessage: String => String,
      requestTraceContext: TraceContext,
  ) extends SimpleForwardingServerCallListener[ReqT](delegate) {

    /** Called when the server receives the request. */
    override def onMessage(message: ReqT): Unit = {
      val traceContext = traceContextOfMessage(message).getOrElse(requestTraceContext)
      logger.debug(
        createLogMessage(
          show"received a message ${cutMessage(message).unquoted}\n  Request ${requestTraceContext.showTraceId}"
        )
      )(traceContext)
      logThrowable(delegate.onMessage(message))(createLogMessage, traceContext)
    }

    /** Called when the client completed all message sending (except for cancellation). */
    override def onHalfClose(): Unit = {
      logger.trace(createLogMessage(s"finished receiving messages"))(requestTraceContext)
      logThrowable(delegate.onHalfClose())(createLogMessage, requestTraceContext)
    }

    /** Called when the client cancels the call. */
    override def onCancel(): Unit = {
      logger.info(createLogMessage("cancelled"))(requestTraceContext)
      logThrowable(delegate.onCancel())(createLogMessage, requestTraceContext)
      cancelled.set(true)
    }

    /** Called when the server considers the call completed. */
    override def onComplete(): Unit = {
      logger.debug(createLogMessage("completed"))(requestTraceContext)
      logThrowable(delegate.onComplete())(createLogMessage, requestTraceContext)
    }

    override def onReady(): Unit = {
      // This call is "just a suggestion" according to the docs and turns out to be quite flaky, even in simple scenarios.
      // Not logging therefore.
      logThrowable(delegate.onReady())(createLogMessage, requestTraceContext)
    }
  }

  /** Intercepts events sent by the server.
    */
  class LoggingServerCall[ReqT, RespT](
      delegate: ServerCall[ReqT, RespT],
      createLogMessage: String => String,
      requestTraceContext: TraceContext,
  ) extends SimpleForwardingServerCall[ReqT, RespT](delegate) {

    /** Called when the server sends the response headers. */
    override def sendHeaders(headers: Metadata): Unit = {
      logger.trace(createLogMessage(s"sending response headers ${cutMessage(headers)}"))(
        requestTraceContext
      )
      delegate.sendHeaders(headers)
    }

    /** Called when the server sends a response. */
    override def sendMessage(message: RespT): Unit = {
      val traceContext = traceContextOfMessage(message).getOrElse(requestTraceContext)
      logger.debug(
        createLogMessage(
          s"sending response ${cutMessage(message)}\n  Request ${requestTraceContext.showTraceId}"
        )
      )(traceContext)
      delegate.sendMessage(message)
    }

    /** Called when the server closes the call. */
    override def close(status: Status, trailers: Metadata): Unit = {
      val enhancedStatus = logStatusOnClose(status, trailers, createLogMessage)(requestTraceContext)
      delegate.close(enhancedStatus, trailers)
    }
  }
}

/** Base class for building gRPC API request loggers.
  * Used in Canton network to build a client-side gRPC API
  * request logger in addition to the server-side one.
  *
  * See https://github.com/DACH-NY/the-real-canton-coin/blob/bea9ccff84e72957aa7ac57ae3d1a00bc6d368d0/canton/community/common/src/main/scala/com/digitalasset/canton/networking/grpc/ApiClientRequestLogger.scala#L16
  *
  * @param config Configuration to tailor the output
  */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class ApiRequestLoggerBase(
    override protected val loggerFactory: NamedLoggerFactory,
    config: ApiLoggingConfig,
) extends NamedLogging {

  private lazy val printer = config.printer

  protected def logThrowable(
      within: => Unit
  )(createLogMessage: String => String, traceContext: TraceContext): Unit = {
    try {
      within
    } catch {
      // If the server implementation fails, the server method must return a failed future or call StreamObserver.onError.
      // This handler is invoked, when an internal GRPC error occurs or the server implementation throws.
      case t: Throwable =>
        logger.error(createLogMessage("failed with an unexpected throwable"), t)(traceContext)
        t match {
          case _: RuntimeException =>
            throw t
          case _: Exception =>
            // Convert to a RuntimeException, because GRPC is unable to handle undeclared checked exceptions.
            throw new RuntimeException(t)
          case _: Throwable =>
            throw t
        }
    }
  }

  protected def logStatusOnClose(
      status: Status,
      trailers: Metadata,
      createLogMessage: String => String,
  )(implicit requestTraceContext: TraceContext): Status = {
    val enhancedStatus = enhance(status)

    val statusString = Option(enhancedStatus.getDescription).filterNot(_.isEmpty) match {
      case Some(d) => s"${enhancedStatus.getCode}/$d"
      case None => enhancedStatus.getCode.toString
    }

    val trailersString = stringOfTrailers(trailers)

    if (enhancedStatus.getCode == Status.OK.getCode) {
      logger.debug(
        createLogMessage(s"succeeded($statusString)$trailersString"),
        enhancedStatus.getCause,
      )
    } else {
      val message = createLogMessage(s"failed with $statusString$trailersString")
      if (enhancedStatus.getCode == UNKNOWN || enhancedStatus.getCode == DATA_LOSS) {
        logger.error(message, enhancedStatus.getCause)
      } else if (enhancedStatus.getCode == INTERNAL) {
        if (enhancedStatus.getDescription == "Half-closed without a request") {
          // If a call is cancelled, GRPC may half-close the call before the first message has been delivered.
          // The result is this status.
          // Logging with INFO to not confuse the user.
          // The status is still delivered to the client, to facilitate troubleshooting if there is a deeper problem.
          logger.info(message, enhancedStatus.getCause)
        } else {
          logger.error(message, enhancedStatus.getCause)
        }
      } else if (enhancedStatus.getCode == UNAUTHENTICATED) {
        logger.debug(message, enhancedStatus.getCause)
      } else {
        logger.info(message, enhancedStatus.getCause)
      }
    }

    enhancedStatus
  }

  @SuppressWarnings(Array("org.wartremover.warts.Product"))
  protected def cutMessage(message: Any): String = {
    if (config.messagePayloads) {
      printer.printAdHoc(message)
    } else ""
  }

  protected def stringOfTrailers(trailers: Metadata): String =
    if (!config.messagePayloads || trailers == null || trailers.keys().isEmpty) {
      ""
    } else {
      s"\n  Trailers: ${stringOfMetadata(trailers)}"
    }

  protected def stringOfMetadata(metadata: Metadata): String =
    if (!config.messagePayloads || metadata == null) {
      ""
    } else {
      metadata.toString.limit(config.maxMetadataSize).toString
    }

  protected def enhance(status: Status): Status = {
    if (status.getDescription == null && status.getCause != null) {
      // Copy the exception message to the status in order to transmit it to the client.
      // If you consider this a security risk:
      // - Exceptions are logged. Therefore, exception messages must not contain confidential data anyway.
      // - Note that scalapb.grpc.Grpc.completeObserver also copies exception messages into the status description.
      //   So removing this method would not mitigate the risk.
      status.withDescription(status.getCause.getLocalizedMessage)
    } else {
      status
    }
  }

  protected def inferRequestTraceContext: TraceContext = {
    val grpcTraceContext = TraceContextGrpc.fromGrpcContext
    if (grpcTraceContext.traceId.isDefined) {
      grpcTraceContext
    } else {
      TraceContext.withNewTraceContext(identity)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  protected def traceContextOfMessage[A](message: Any): Option[TraceContext] = {
    import scala.language.reflectiveCalls
    for {
      maybeTraceContextP <- Try(
        message
          .asInstanceOf[{ def traceContext: Option[com.digitalasset.canton.v30.TraceContext] }]
          .traceContext
      ).toOption
      tc <- ProtoConverter.required("traceContextOfMessage", maybeTraceContextP).toOption
      traceContext <- SerializableTraceContext.fromProtoV30(tc).toOption
    } yield traceContext.unwrap
  }
}
