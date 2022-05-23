
// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.RateLimitingInterceptor.serverReflectionInfo
import com.daml.platform.usermanagement.RateLimitingConfig
import io.grpc.Status.Code
import io.grpc._
import io.grpc.protobuf.StatusProto

private[apiserver] final class RateLimitingInterceptor(metrics: Metrics, config: RateLimitingConfig) extends ServerInterceptor {

  import metrics.daml.lapi.threadpool.apiServices

  /** Match naming in [[com.codahale.metrics.InstrumentedExecutorService]] */
  private val submitted = metrics.registry.meter(MetricRegistry.name(apiServices, "submitted"))
  private val running = metrics.registry.meter(MetricRegistry.name(apiServices, "running"))
  private val completed = metrics.registry.meter(MetricRegistry.name(apiServices, "completed"))

  override def interceptCall[ReqT, RespT](
                                           call: ServerCall[ReqT, RespT],
                                           headers: Metadata,
                                           next: ServerCallHandler[ReqT, RespT],
                                         ): ServerCall.Listener[ReqT] = {

    val queued = submitted.getCount-running.getCount-completed.getCount

    val fullMethodName = call.getMethodDescriptor.getFullMethodName

    if (queued > config.maxApiServicesQueueSize && (fullMethodName != serverReflectionInfo)) {
      val rpcStatus = com.google.rpc.Status
        .newBuilder()
        .setCode(Code.ABORTED.value())
        .setMessage(s"The api services queue size ($queued) has exceeded the maximum (${config.maxApiServicesQueueSize}).  Api services metrics are available at $apiServices")
        .build()

      val exception = StatusProto.toStatusRuntimeException(rpcStatus)

      call.close(exception.getStatus, exception.getTrailers)
      new ServerCall.Listener[ReqT]() {}
    } else {
      next.startCall(call, headers)
    }

  }


}

object RateLimitingInterceptor {
  val serverReflectionInfo = "grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"
}