// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor, Status}

class TruncatedStatusInterceptor(maximumDescriptionLength: Int) extends ServerInterceptor {
  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] =
    next.startCall(
      new SimpleForwardingServerCall[ReqT, RespT](call) {
        override def close(status: Status, trailers: Metadata): Unit = {
          val truncatedStatus = status.withDescription(truncate(status.getDescription))
          super.close(truncatedStatus, trailers)
        }
      },
      headers,
    )

  private def truncate(description: String): String =
    if (description != null && description.length > maximumDescriptionLength)
      description.substring(0, maximumDescriptionLength - 3) + "..."
    else
      description
}
