// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.RateLimitingInterceptor.doNonLimit
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import io.grpc.Status.Code
import io.grpc._
import io.grpc.protobuf.StatusProto
import org.slf4j.LoggerFactory

private[apiserver] final class RateLimitingInterceptor(metrics: Metrics, config: RateLimitingConfig)
    extends ServerInterceptor {

  import metrics.daml.lapi.threadpool.apiServices

  private val logger = LoggerFactory.getLogger(getClass)

  /** Match naming in [[com.codahale.metrics.InstrumentedExecutorService]] */
  private val submitted = metrics.registry.meter(MetricRegistry.name(apiServices, "submitted"))
  private val running = metrics.registry.counter(MetricRegistry.name(apiServices, "running"))
  private val completed = metrics.registry.meter(MetricRegistry.name(apiServices, "completed"))

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {

    val queued = submitted.getCount - running.getCount - completed.getCount

    val fullMethodName = call.getMethodDescriptor.getFullMethodName

    if (queued > config.maxApiServicesQueueSize && !doNonLimit.contains(fullMethodName)) {

      val rpcStatus = com.google.rpc.Status
        .newBuilder()
        .setCode(Code.ABORTED.value())
        .setMessage(
          s"""
            | The api services queue size ($queued) has exceeded the maximum (${config.maxApiServicesQueueSize}).
            | The rejected call was $fullMethodName.
            | Api services metrics are available at $apiServices.
          """.stripMargin
        )
        .build()

      logger.info(s"gRPC call rejected: $rpcStatus")

      val exception = StatusProto.toStatusRuntimeException(rpcStatus)

      call.close(exception.getStatus, exception.getTrailers)
      new ServerCall.Listener[ReqT]() {}
    } else {
      next.startCall(call, headers)
    }

  }

}

object RateLimitingInterceptor {
  val doNonLimit = Set(
    "grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
    "grpc.health.v1.Health/Check",
    "grpc.health.v1.Health/Watch",
  )
}
