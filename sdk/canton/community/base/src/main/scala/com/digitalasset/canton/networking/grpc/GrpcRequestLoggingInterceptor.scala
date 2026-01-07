// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.logging.audit.ResponseKind.SevereError
import com.digitalasset.canton.logging.audit.{ApiRequestLogger, ResponseKind, TransportType}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.ShowUtil.*
import com.google.common.annotations.VisibleForTesting
import io.grpc.*
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.Status.Code.*
import io.grpc.inprocess.InProcessSocketAddress

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try

/** Server side interceptor that logs incoming and outgoing traffic.
  *
  * @param config
  *   Configuration to tailor the output
  */
class GrpcRequestLoggingInterceptor(
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
    val requestTraceContext: TraceContext = TraceContextGrpc.fromGrpcContextOrNew("logger")
    val remoteAddr = call.getAttributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)
    val method = call.getMethodDescriptor.getFullMethodName

    val (transport, clientAddr) = GrpcAddressHelper.extractTransport(remoteAddr)
    val callMetadata = CallMetadata(
      apiEndpoint = show"${method.readableLoggerName(config.maxMethodLength)}",
      transport = transport,
      remoteAddress = clientAddr,
    )

    auditLogger.traceMessage(callMetadata, s"received headers ${stringOfMetadata(headers)}")(
      requestTraceContext
    )

    val loggingServerCall =
      new LoggingServerCall(call, callMetadata, requestTraceContext)
    val serverCallListener = next.startCall(loggingServerCall, headers)
    new LoggingServerCallListener(
      serverCallListener,
      callMetadata,
      requestTraceContext,
    )
  }

  /** Intercepts events sent by the client.
    */
  class LoggingServerCallListener[ReqT, RespT](
      delegate: ServerCall.Listener[ReqT],
      callMetadata: CallMetadata,
      requestTraceContext: TraceContext,
  ) extends SimpleForwardingServerCallListener[ReqT](delegate) {

    /** Called when the server receives the request. */
    override def onMessage(message: ReqT): Unit = {
      val traceContext = traceContextOfMessage(message).getOrElse(requestTraceContext)
      auditLogger.logIncomingRequest(
        callMetadata,
        message,
      )(
        traceContext
      )
      logThrowable(delegate.onMessage(message))(callMetadata, traceContext)
    }

    /** Called when the client completed all message sending (except for cancellation). */
    override def onHalfClose(): Unit = {
      auditLogger.traceMessage(
        callMetadata,
        "finished receiving messages",
      )(requestTraceContext)
      logThrowable(delegate.onHalfClose())(callMetadata, requestTraceContext)
    }

    /** Called when the client cancels the call. */
    override def onCancel(): Unit = {
      auditLogger.logResponseStatus(
        callMetadata,
        ResponseKind.MinorError,
        "cancelled",
        None,
      )(requestTraceContext)
      logThrowable(delegate.onCancel())(callMetadata, requestTraceContext)
      cancelled.set(true)
    }

    /** Called when the server considers the call completed. */
    override def onComplete(): Unit = {
      auditLogger.debugMessage(
        callMetadata,
        "completed",
      )(requestTraceContext)
      logThrowable(delegate.onComplete())(callMetadata, requestTraceContext)
    }

    override def onReady(): Unit =
      // This call is "just a suggestion" according to the docs and turns out to be quite flaky, even in simple scenarios.
      // Not logging therefore.
      logThrowable(delegate.onReady())(callMetadata, requestTraceContext)
  }

  /** Intercepts events sent by the server.
    */
  class LoggingServerCall[ReqT, RespT](
      delegate: ServerCall[ReqT, RespT],
      callMetadata: CallMetadata,
      requestTraceContext: TraceContext,
  ) extends SimpleForwardingServerCall[ReqT, RespT](delegate) {

    /** Called when the server sends the response headers. */
    override def sendHeaders(headers: Metadata): Unit = {
      auditLogger.traceMessage(
        callMetadata,
        s"sending response headers",
        Some(headers.toString),
      )(requestTraceContext)
      delegate.sendHeaders(headers)
    }

    /** Called when the server sends a response. */
    override def sendMessage(message: RespT): Unit = {
      val traceContext = traceContextOfMessage(message).getOrElse(requestTraceContext)
      auditLogger.debugMessage(
        callMetadata,
        s"sending response",
        Some(message),
      )(traceContext)
      delegate.sendMessage(message)
    }

    /** Called when the server closes the call. */
    override def close(status: Status, trailers: Metadata): Unit = {

      val enhancedStatus = logStatusOnClose(
        status,
        trailers,
        callMetadata,
      )(requestTraceContext)
      delegate.close(enhancedStatus, trailers)
    }
  }
}

/** Base class for building gRPC API request loggers. Used in Canton network to build a client-side
  * gRPC API request logger in addition to the server-side one.
  *
  * See
  * https://github.com/DACH-NY/the-real-canton-coin/blob/bea9ccff84e72957aa7ac57ae3d1a00bc6d368d0/canton/community/common/src/main/scala/com/digitalasset/canton/networking/grpc/ApiClientRequestLogger.scala#L16
  *
  * @param config
  *   Configuration to tailor the output
  */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class ApiRequestLoggerBase(
    override protected val loggerFactory: NamedLoggerFactory,
    config: ApiLoggingConfig,
) extends NamedLogging {

  protected val auditLogger = new ApiRequestLogger(config, loggerFactory)

  protected def logThrowable(
      within: => Unit
  )(callMetadata: CallMetadata, traceContext: TraceContext): Unit =
    try {
      within
    } catch {
      // If the server implementation fails, the server method must return a failed future or call StreamObserver.onError.
      // This handler is invoked, when an internal GRPC error occurs or the server implementation throws.
      case t: Throwable =>
        auditLogger
          .logResponseStatus(
            callMetadata,
            SevereError,
            s"failed with an unexpected throwable",
            Some(t),
          )(traceContext)

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

  protected def logStatusOnClose(
      status: Status,
      trailers: Metadata,
      call: CallMetadata,
  )(implicit requestTraceContext: TraceContext): Status = {
    val enhancedStatus = enhance(status)

    val statusString = Option(enhancedStatus.getDescription).filterNot(_.isEmpty) match {
      case Some(d) => s"${enhancedStatus.getCode}/$d"
      case None => enhancedStatus.getCode.toString
    }

    val trailersString = stringOfTrailers(trailers)
    val statusMessage = s"succeeded($statusString)$trailersString"

    if (enhancedStatus.getCode == Status.OK.getCode) {
      auditLogger.logResponseStatus(
        call,
        ResponseKind.OK,
        statusMessage,
        None,
      )(requestTraceContext)
    } else {
      val message = s"failed with $statusString$trailersString"
      if (
        enhancedStatus.getCode == UNKNOWN
        || enhancedStatus.getCode == DATA_LOSS
        || enhancedStatus.getCode == INTERNAL
      ) {
        auditLogger.logResponseStatus(
          call,
          ResponseKind.SevereError,
          message,
          Some(enhancedStatus.getCause),
        )(requestTraceContext)
      } else if (
        enhancedStatus.getCode == UNAUTHENTICATED || enhancedStatus.getCode == PERMISSION_DENIED
      ) {
        auditLogger.logResponseStatus(
          call,
          ResponseKind.Security,
          message,
          Some(enhancedStatus.getCause),
        )(requestTraceContext)
      } else {
        auditLogger.logResponseStatus(
          call,
          ResponseKind.MinorError,
          message,
          Some(enhancedStatus.getCause),
        )(requestTraceContext)
      }
    }
    enhancedStatus
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

  protected def enhance(status: Status): Status =
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
object GrpcAddressHelper {
  def extractTransport(
      remoteAddr: SocketAddress
  ): (TransportType, Either[String, InetSocketAddress]) =
    remoteAddr match {
      case inet: InetSocketAddress =>
        (TransportType.Grpc, Right(inet))
      case internal: InProcessSocketAddress =>
        (TransportType.InProcessGrpc, Left(internal.toString()))
      case other =>
        (TransportType.Grpc, Left(s"unknown: $other"))
    }
}

/** Metadata collected about an Api call for logging purposes. apiEndpoint: The API endpoint being
  * called, e.g., "com.daml.ledger.api.v1.CommandService/SubmitAndWait" transport: The transport
  * type used for the call, e.g., gRPC or HTTP remoteAddress: The remote address of the caller,
  * InetSocketAddress, if not possible to retrieve a string error/identifier
  */

final case class CallMetadata(
    apiEndpoint: String,
    transport: TransportType,
    remoteAddress: Either[String, InetSocketAddress],
)
