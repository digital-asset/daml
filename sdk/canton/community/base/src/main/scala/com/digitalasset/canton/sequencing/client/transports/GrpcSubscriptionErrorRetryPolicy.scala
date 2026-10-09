// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.sequencing.client.CheckedSubscriptionErrorRetryPolicy
import com.digitalasset.canton.tracing.TraceContext

import GrpcSubscriptionErrorRetryPolicy.*

class GrpcSubscriptionErrorRetryPolicy(protected val loggerFactory: NamedLoggerFactory)
    extends CheckedSubscriptionErrorRetryPolicy[GrpcSubscriptionError]
    with NamedLogging {
  override protected def retryInternal(error: GrpcSubscriptionError, receivedItems: Boolean)(
      implicit traceContext: TraceContext
  ): Boolean = logAndDetermineRetry(error.grpcError)
}

object GrpcSubscriptionErrorRetryPolicy {
  implicit class EnhancedGrpcStatus(val status: io.grpc.Status) extends AnyVal {
    def hasClosedChannelExceptionCause: Boolean = status.getCause match {
      case _: java.nio.channels.ClosedChannelException => true
      case _ => false
    }
  }

  private[transports] def logAndDetermineRetry(
      grpcError: GrpcError
  )(implicit loggingContext: ErrorLoggingContext): Boolean =
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

      case _: GrpcError.GrpcServerError =>
        // We believe these errors (INTERNAL, UNKNOWN, DATA_LOSS) can in some circumstances by transient, and
        // therefore we err on the side of caution and retry.
        loggingContext.debug(
          s"Subscription failed with ${grpcError.status}. Retrying subscription as this could be transient."
        )
        true

      case _: GrpcError.GrpcClientGaveUp | _: GrpcError.GrpcClientError =>
        loggingContext.info("Not reconnecting.")
        false
    }
}
