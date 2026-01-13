// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.metrics.grpc.GrpcMetricsServerInterceptor
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.error.ErrorInterceptor
import io.grpc.ServerInterceptor

object GrpcInterceptors {
  // Unfortunately, we can't get the maximum inbound message size from the client, so we don't know
  // how big this should be. This seems long enough to contain useful data, but short enough that it
  // won't break most well-configured clients.
  // As the default response header limit for a Netty client is 8 KB, we set our limit to 4 KB to
  // allow for extra information such as the exception stack trace.
  private val MaximumStatusDescriptionLength = 4 * 1024 // 4 KB
  def apply(
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
      interceptors: List[ServerInterceptor] = List.empty,
  ): List[ServerInterceptor] = interceptors ::: List(
    new GrpcMetricsServerInterceptor(metrics.grpc),
    new TruncatedStatusInterceptor(MaximumStatusDescriptionLength),
    new ErrorInterceptor(loggerFactory),
  )
}
