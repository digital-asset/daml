// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.{MetricName, Metrics}
import com.daml.platform.apiserver.RateLimitingInterceptor.doNonLimit
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.configuration.ServerRole
import io.grpc.Status.Code
import io.grpc._
import io.grpc.protobuf.StatusProto
import org.slf4j.LoggerFactory

private[apiserver] final class RateLimitingInterceptor(metrics: Metrics, config: RateLimitingConfig)
    extends ServerInterceptor {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Match naming in [[com.codahale.metrics.InstrumentedExecutorService]] */
  private case class InstrumentedCount(name: String, prefix: MetricName) {
    private val submitted = metrics.registry.meter(MetricRegistry.name(prefix, "submitted"))
    private val running = metrics.registry.counter(MetricRegistry.name(prefix, "running"))
    private val completed = metrics.registry.meter(MetricRegistry.name(prefix, "completed"))
    def queueSize: Long = submitted.getCount - running.getCount - completed.getCount
  }

  private val apiServices =
    InstrumentedCount("Api Services", metrics.daml.lapi.threadpool.apiServices)
  private val indexDbThreadpool = InstrumentedCount(
    "Index Database Connection Threadpool",
    MetricName(metrics.daml.index.db.threadpool.connection, ServerRole.ApiServer.threadPoolSuffix),
  )

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {

    serviceOverloaded(call.getMethodDescriptor.getFullMethodName) match {
      case Some(errorMessage) =>
        val rpcStatus = com.google.rpc.Status
          .newBuilder()
          .setCode(Code.ABORTED.value())
          .setMessage(errorMessage)
          .build()

        logger.info(s"gRPC call rejected: $rpcStatus")

        val exception = StatusProto.toStatusRuntimeException(rpcStatus)

        call.close(exception.getStatus, exception.getTrailers)
        new ServerCall.Listener[ReqT]() {}

      case None =>
        next.startCall(call, headers)
    }

  }

  private def serviceOverloaded(fullMethodName: String): Option[String] = {
    if (doNonLimit.contains(fullMethodName)) {
      None
    } else {
      (for {
        _ <- metricOverloaded(fullMethodName, apiServices, config.maxApiServicesQueueSize)
        _ <- metricOverloaded(
          fullMethodName,
          indexDbThreadpool,
          config.maxApiServicesIndexDbQueueSize,
        )
      } yield ()).fold(Some.apply, _ => None)
    }
  }

  private def metricOverloaded(
      fullMethodName: String,
      count: InstrumentedCount,
      limit: Int,
  ): Either[String, Unit] = {
    val queued = count.queueSize
    if (queued > limit) {
      val rpcStatus =
        s"""
           | The ${count.name} queue size ($queued) has exceeded the maximum ($limit).
           | The rejected call was $fullMethodName.
           | Api services metrics are available at ${count.prefix}.
          """.stripMargin

      Left(rpcStatus)
    } else {
      Right(())
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
