// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.sequencing.client.CheckedSubscriptionErrorRetryPolicy
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Status

import GrpcSubscriptionErrorRetryPolicy.*

class GrpcSubscriptionErrorRetryPolicy(protected val loggerFactory: NamedLoggerFactory)
    extends CheckedSubscriptionErrorRetryPolicy[GrpcSubscriptionError]
    with NamedLogging {
  override protected def retryInternal(error: GrpcSubscriptionError, receivedItems: Boolean)(
      implicit traceContext: TraceContext
  ): Boolean = logAndDetermineRetry(error.grpcError, receivedItems)
}

object GrpcSubscriptionErrorRetryPolicy {
  implicit class EnhancedGrpcStatus(val status: io.grpc.Status) extends AnyVal {
    def hasClosedChannelExceptionCause: Boolean = status.getCause match {
      case _: java.nio.channels.ClosedChannelException => true
      case _ => false
    }
  }

  private[transports] def logAndDetermineRetry(
      grpcError: GrpcError,
      receivedItems: Boolean,
  )(implicit loggingContext: ErrorLoggingContext): Boolean = {
    grpcError match {
      case _: GrpcError.GrpcServiceUnavailable =>
        val causes = Seq(grpcError.status.getDescription) ++ GrpcError.collectCauses(
          Option(grpcError.status.getCause)
        )
        loggingContext.info(
          s"Trying to reconnect to give the sequencer the opportunity to become available again (after ${causes
              .mkString(", ")})"
        )
        true

      case error: GrpcError.GrpcRequestRefusedByServer =>
        val retry = error.isAuthenticationTokenMissing
        if (retry)
          loggingContext.info(
            s"Trying to reconnect to give the sequencer the opportunity to refresh the authentication token."
          )
        else
          loggingContext.debug("Not trying to reconnect.")
        retry

      case serverError: GrpcError.GrpcServerError
          if receivedItems && serverError.status.getCode == Status.INTERNAL.getCode =>
        // a connection reset by an intermediary can cause GRPC to raise an INTERNAL error.
        // (this is seen when the GCloud load balancer times out subscriptions on the global domain)
        // if we've received any items during the course of the subscription we will assume its fine to reconnect.
        // if there is actually an application issue with the server, we'd expect it to immediately fail and then
        // it will not retry its connection
        loggingContext.debug(
          s"After successfully receiving some events the sequencer subscription received an error. Retrying subscription."
        )
        true

      case serverError: GrpcError.GrpcServerError
          if serverError.status.getCode == Status.UNKNOWN.getCode && serverError.status.hasClosedChannelExceptionCause =>
        // In this conversation https://gitter.im/grpc/grpc?at=5f464aa854288c687ee06a25
        // someone who maintains the grpc codebase explains:
        // "'Channel closed' is when we have no knowledge as to what went wrong; it could be anything".
        // In practice, we've seen this peculiar error sometimes appear when the sequencer goes unavailable,
        // so let's make sure to retry.
        loggingContext.debug(
          s"Closed channel exception can appear when the server becomes unavailable. Retrying."
        )
        true
      case _: GrpcError.GrpcClientGaveUp | _: GrpcError.GrpcClientError |
          _: GrpcError.GrpcServerError =>
        loggingContext.info("Not reconnecting.")
        false
    }
  }
}
