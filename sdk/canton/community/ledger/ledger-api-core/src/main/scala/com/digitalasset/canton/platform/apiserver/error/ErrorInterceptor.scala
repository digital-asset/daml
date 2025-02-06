// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.error

import com.daml.error.{BaseError, NoLogging}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.logging.*
import io.grpc.*
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall

import scala.util.control.NonFatal

final class ErrorInterceptor(val loggerFactory: NamedLoggerFactory)
    extends ServerInterceptor
    with NamedLogging {

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {

    val forwardingCall = new SimpleForwardingServerCall[ReqT, RespT](call) {

      /** Here we are trying to detect status/trailers pairs that:
        * - originated from the server implementation (i.e. Participant services as opposed to internal to gRPC implementation) AND
        * - did not originate from LAPI error codes.
        * NOTE: We are not attempting to detect if a status/trailers pair originates from exceptional conditions within gRPC implementation itself.
        *
        * We are handling unary endpoints that returned failed Futures or other direct invocation of [[io.grpc.stub.StreamObserver#onError]].
        * We are NOT handling here exceptions thrown outside of Futures or Pekko streams. These are handled separately in
        * [[com.digitalasset.canton.platform.apiserver.error.ErrorListener]].
        * We are NOT handling here exceptions thrown inside Pekko streaming. These are handled in
        * [[com.daml.grpc.adapter.server.pekko.ServerAdapter.toSink]]
        *
        * Details:
        * 1. Handling of Status.INTERNAL:
        * The gRPC services that we generate via scalapb are using [[scalapb.grpc.Grpc.completeObserver]]
        * when bridging from Future[T] and into io.grpc.stub.StreamObserver[T].
        * [[scalapb.grpc.Grpc.completeObserver]] does the following:
        * a) propagates instances of StatusException and StatusRuntimeException without changes,
        * b) translates other throwables into a StatusException with Status.INTERNAL.
        * We assume that we don't need to deal with a) but need to detect and deal with b).
        * Knowing that Status.INTERNAL is used only by [[com.daml.error.ErrorCategory.SystemInternalAssumptionViolated]],
        * which is marked a security sensitive, we have the following heuristic: check whether gRPC status is Status.INTERNAL
        * and gRPC status description is not security sanitized.
        * 2. Handling of Status.UNKNOWN:
        * We do not have an error category that uses UNKNOWN so there is no risk of catching a legitimate Ledger API error code.
        * A Status.UNKNOWN can arise when someone (us or a library):
        * - calls [[io.grpc.stub.StreamObserver#onError]] providing an exception that is not a Status(Runtime)Exception
        *   (see [[io.grpc.stub.ServerCalls.ServerCallStreamObserverImpl#onError]] and [[io.grpc.Status#fromThrowable]]),
        * - calls [[io.grpc.ServerCall#close]] providing status.UNKNOWN.
        */
      override def close(status: Status, trailers: Metadata): Unit =
        if (isUnsanitizedInternal(status) || status.getCode == Status.Code.UNKNOWN) {
          val recreatedException = status.asRuntimeException(trailers)
          val errorCodeException = LedgerApiErrors.InternalError
            .UnexpectedOrUnknownException(t = recreatedException)(NoLogging)
            .asGrpcError
          // Retrieving status and metadata in the same way as in `io.grpc.stub.ServerCalls.ServerCallStreamObserverImpl.onError`.
          val newMetadata =
            Option(Status.trailersFromThrowable(errorCodeException)).getOrElse(new Metadata())
          val newStatus = Status.fromThrowable(errorCodeException)
          LogOnUnhandledFailureInClose(logger, superClose(newStatus, newMetadata))
        } else {
          LogOnUnhandledFailureInClose(logger, superClose(status, trailers))
        }

      /** This method serves as an accessor to the super.close() which facilitates its access from the outside of this class.
        * This is needed in order to allow the call to be captured in the closure passed to the [[LogOnUnhandledFailureInClose]]
        * error handler.
        *
        * As at Scala 2.13.8, not using this redirection results in a runtime IllegalAccessError.
        * Remove this redirection once the runtime exception can be avoided.
        */
      private def superClose(status: Status, trailers: Metadata): Unit =
        super.close(status, trailers)
    }

    val listener = next.startCall(forwardingCall, headers)
    new ErrorListener(
      loggerFactory = loggerFactory,
      delegate = listener,
      call = call,
    )
  }

  private def isUnsanitizedInternal(status: Status): Boolean =
    status.getCode == Status.Code.INTERNAL &&
      (status.getDescription == null ||
        !BaseError.isRedactedMessage(
          status.getDescription
        ))
}

class ErrorListener[ReqT, RespT](
    val loggerFactory: NamedLoggerFactory,
    delegate: ServerCall.Listener[ReqT],
    call: ServerCall[ReqT, RespT],
) extends ForwardingServerCallListener.SimpleForwardingServerCallListener[ReqT](delegate)
    with NamedLogging {

  /** Handles errors arising outside Futures or Pekko streaming.
    *
    * NOTE: We don't override other listener methods: onCancel, onComplete, onReady and onMessage;
    * as it seems overriding only onHalfClose is sufficient.
    */
  override def onHalfClose(): Unit =
    try {
      super.onHalfClose()
    } catch {
      // For StatusException and StatusRuntimeException:
      // 1. Assuming `t` was produced by self-service error codes and, thus, deeming the corresponding status and
      //    trailers do not need security sanitization.
      // 2. We need to catch it and call `call.close` as otherwise gRPC will close the stream with a Status.UNKNOWN
      //    (see io.grpc.internal.ServerImpl.JumpToApplicationThreadServerStreamListener.internalClose)
      case t: StatusException =>
        LogOnUnhandledFailureInClose(logger, call.close(t.getStatus, t.getTrailers))
      case t: StatusRuntimeException =>
        LogOnUnhandledFailureInClose(logger, call.close(t.getStatus, t.getTrailers))
      case NonFatal(t) =>
        val e = LedgerApiErrors.InternalError
          .UnexpectedOrUnknownException(t = t)(NoLogging)
          .asGrpcError
        LogOnUnhandledFailureInClose(logger, call.close(e.getStatus, e.getTrailers))
    }
}

private[error] object LogOnUnhandledFailureInClose {

  def apply[T](logger: TracedLogger, close: => T): T =
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
          )(ErrorLoggingContext(logger, LoggingContextWithTrace.empty))
          .discard
        throw e
    }
}
