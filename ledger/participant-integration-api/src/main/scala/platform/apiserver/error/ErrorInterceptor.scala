// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.error

import com.daml.error.{BaseError, DamlContextualizedErrorLogger}
import com.daml.error.definitions.LedgerApiErrors
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.{
  ForwardingServerCallListener,
  Metadata,
  ServerCall,
  ServerCallHandler,
  ServerInterceptor,
  Status,
  StatusException,
  StatusRuntimeException,
}

final class ErrorInterceptor extends ServerInterceptor {

  private val logger = ContextualizedLogger.get(getClass)
  private val emptyLoggingContext = LoggingContext.newLoggingContext(identity)

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {

    val forwardingCall = new SimpleForwardingServerCall[ReqT, RespT](call) {

      /** Here we are trying to detect status/trailers pairs that:
        * - originated from the server implementation (i.e. Participant services as opposed to internal to gRPC implementation) AND
        * - did not originate from self-service errors infrastructure.
        * NOTE: We are not attempting to detect if a status/trailers pair originates from exceptional conditions within gRPC implementation itself.
        *
        * We are handling unary endpoints that returned failed Futures,
        * We are NOT handling here exceptions thrown outside of Futures or Akka streams. These are handled separately in
        * [[com.daml.platform.apiserver.error.ErrorListener]].
        * We are NOT handling here exceptions thrown inside Akka streaming. These are handled in
        * [[com.daml.grpc.adapter.server.akka.ServerAdapter.toSink]]
        *
        * Details:
        * The gRPC services that we generate via scalapb are using [[scalapb.grpc.Grpc.completeObserver]]
        * when bridging from Future[T] and into io.grpc.stub.StreamObserver[T].
        * [[scalapb.grpc.Grpc.completeObserver]] does the following:
        * a) propagates instances of StatusException and StatusRuntimeException without changes,
        * b) translates other throwables into a StatusException with Status.INTERNAL.
        * We assume that we don't need to deal with a) but need to detect and deal with b).
        * Knowing that Status.INTERNAL is used only by [[com.daml.error.ErrorCategory.SystemInternalAssumptionViolated]],
        * which is marked a security sensitive, we have the following heuristic: check whether gRPC status is Status.INTERNAL
        * and gRPC status description is not security sanitized.
        */
      override def close(status: Status, trailers: Metadata): Unit = {
        if (
          status.getCode == Status.Code.INTERNAL &&
          (status.getDescription == null || !BaseError.isSanitizedSecuritySensitiveMessage(
            status.getDescription
          ))
        ) {
          val recreatedException = status.asRuntimeException(trailers)
          val selfServiceException = LedgerApiErrors.InternalError
            .UnexpectedOrUnknownException(t = recreatedException)(
              new DamlContextualizedErrorLogger(logger, emptyLoggingContext, None)
            )
            .asGrpcError
          // Retrieving status and metadata in the same way as in `io.grpc.stub.ServerCalls.ServerCallStreamObserverImpl.onError`.
          val newMetadata =
            Option(Status.trailersFromThrowable(selfServiceException)).getOrElse(new Metadata())
          val newStatus = Status.fromThrowable(selfServiceException)
          super.close(newStatus, newMetadata)
        } else {
          super.close(status, trailers)
        }
      }

    }
    val listener = next.startCall(forwardingCall, headers)
    new ErrorListener(
      delegate = listener,
      call = call,
    )
  }

}

class ErrorListener[ReqT, RespT](delegate: ServerCall.Listener[ReqT], call: ServerCall[ReqT, RespT])
    extends ForwardingServerCallListener.SimpleForwardingServerCallListener[ReqT](delegate) {

  private val logger = ContextualizedLogger.get(getClass)
  private val emptyLoggingContext = LoggingContext.newLoggingContext(identity)

  /** Handles errors arising outside Futures or Akka streaming.
    *
    * NOTE: We don't override other listener methods: onCancel, onComplete, onReady and onMessage;
    * as it seems overriding only onHalfClose is sufficient.
    */
  override def onHalfClose(): Unit = {
    try {
      super.onHalfClose()
    } catch {
      // For StatusException and StatusRuntimeException:
      // 1. Assuming `t` was produced by self-service error codes and, thus, deeming the corresponding status and
      //    trailers do not need security sanitization.
      // 2. We need to catch it and call `call.close` as otherwise gRPC will close the stream with a Status.UNKNOWN
      //    (see io.grpc.internal.ServerImpl.JumpToApplicationThreadServerStreamListener.internalClose)
      case t: StatusException =>
        call.close(t.getStatus, t.getTrailers)
      case t: StatusRuntimeException =>
        call.close(t.getStatus, t.getTrailers)
      case t: Throwable =>
        val e = LedgerApiErrors.InternalError
          .UnexpectedOrUnknownException(t = t)(
            new DamlContextualizedErrorLogger(logger, emptyLoggingContext, None)
          )
          .asGrpcError
        call.close(e.getStatus, e.getTrailers)
    }

  }

}
