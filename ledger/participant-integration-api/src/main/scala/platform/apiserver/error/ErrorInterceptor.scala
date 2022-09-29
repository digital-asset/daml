// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.error

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{BaseError, DamlContextualizedErrorLogger}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc._

import scala.util.control.NonFatal

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
          LogOnUnhandledFailureInClose(closeWithCopiedMetadata(newStatus, newMetadata))
        } else {
          LogOnUnhandledFailureInClose(closeWithCopiedMetadata(status, trailers))
        }
      }

      /** This method serves as an accessor to the super.close() which facilitates its access from the outside of this class.
        * This is needed in order to allow the call to be captured in the closure passed to the [[LogOnUnhandledFailureInClose]]
        * error handler.
        *
        * TODO As of Scala 2.13.8, not using this redirection results in a runtime IllegalAccessError.
        *      Remove this redirection once the runtime exception can be avoided.
        */
      private def closeWithCopiedMetadata(status: Status, trailers: Metadata): Unit = {
        val copiedMetadata = MetadataUtils.copy(trailers)
        super.close(status, copiedMetadata)
      }
    }

    val listener = next.startCall(forwardingCall, headers)

    new ErrorListener(
      delegate = listener,
      call = forwardingCall,
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
        LogOnUnhandledFailureInClose(call.close(t.getStatus, t.getTrailers))
      case t: StatusRuntimeException =>
        LogOnUnhandledFailureInClose(call.close(t.getStatus, t.getTrailers))
      case t: Throwable =>
        val e = LedgerApiErrors.InternalError
          .UnexpectedOrUnknownException(t = t)(
            new DamlContextualizedErrorLogger(logger, emptyLoggingContext, None)
          )
          .asGrpcError
        LogOnUnhandledFailureInClose(call.close(e.getStatus, e.getTrailers))
    }
  }
}

private[error] object LogOnUnhandledFailureInClose {
  private val logger = ContextualizedLogger.get(getClass)

  def apply[T](close: => T): T = {
    // If close throws, we can't call ServerCall.close a second time
    // since it might have already been marked internally as closed.
    // In this situation, we can't do much about it except for notifying the participant operator.
    try close
    catch {
      case NonFatal(e) =>
        // Instantiate the self-service error code as it logs the error on creation.
        // This error is considered security-sensitive and can't be propagated to the client.
        LedgerApiErrors.InternalError
          .Generic(
            s"Unhandled error in ${classOf[ServerCall[_, _]].getSimpleName}.close(). " +
              s"The gRPC client might have not been notified about the call/stream termination. " +
              s"Either notify clients to retry pending unary/streaming calls or restart the participant server.",
            Some(e),
          )(new DamlContextualizedErrorLogger(logger, LoggingContext.empty, None))
        throw e
    }
  }
}
