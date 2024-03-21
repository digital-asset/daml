// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc

import java.util.concurrent.atomic.AtomicReference

import com.daml.grpc.interceptors.ForwardingServerCallListener
import com.daml.grpc.test.GrpcServer
import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor, StatusRuntimeException}
import io.grpc.health.v1.{HealthCheckRequest, HealthGrpc}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

final class ReverseProxySpec extends AsyncFlatSpec with Matchers with GrpcServer with Inside {

  import ReverseProxySpec._
  import Services._

  behavior of "ReverseProxy.create"

  it should "fail if the backend does not support reflection" in withServices(
    Health.newInstance
  ) { backend =>
    val proxyBuilder = InProcessServerBuilder.forName(InProcessServerBuilder.generateName())
    ReverseProxy
      .owner(backend, proxyBuilder, interceptors = Map.empty)
      .use(_ => fail("The proxy started but the backend does not support reflection"))
      .failed
      .map(_ shouldBe a[StatusRuntimeException])
  }

  it should "expose the backend's own services" in withServices(
    Health.newInstance,
    Reflection.newInstance,
  ) { backend =>
    val proxyName = InProcessServerBuilder.generateName()
    val proxyBuilder = InProcessServerBuilder.forName(proxyName)
    ReverseProxy.owner(backend, proxyBuilder, interceptors = Map.empty).use { _ =>
      val proxy = InProcessChannelBuilder.forName(proxyName).build()
      Health.getHealthStatus(backend) shouldEqual Health.getHealthStatus(proxy)
      Reflection.listServices(backend) shouldEqual Reflection.listServices(proxy)
    }
  }

  it should "correctly set up an interceptor" in withServices(
    Health.newInstance,
    Reflection.newInstance,
  ) { backend =>
    val proxyName = InProcessServerBuilder.generateName()
    val proxyBuilder = InProcessServerBuilder.forName(proxyName)
    val recorder = new RecordingInterceptor
    ReverseProxy
      .owner(backend, proxyBuilder, Map(HealthGrpc.SERVICE_NAME -> Seq(recorder)))
      .use { _ =>
        val proxy = InProcessChannelBuilder.forName(proxyName).build()
        Health.getHealthStatus(backend)
        recorder.latestRequest() shouldBe empty
        Health.getHealthStatus(proxy)
        inside(recorder.latestRequest()) { case Some(request: Array[Byte]) =>
          HealthCheckRequest.parseFrom(request)
          succeed
        }
      }
  }

}

object ReverseProxySpec {

  final class Callback[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
      callback: ReqT => Unit,
  ) extends ForwardingServerCallListener(call, headers, next) {
    override def onMessage(message: ReqT): Unit = {
      callback(message)
      super.onMessage(message)
    }
  }

  final class RecordingInterceptor extends ServerInterceptor {

    private val latestRequestReference = new AtomicReference[Any]()

    def latestRequest(): Option[Any] = Option(latestRequestReference.get)

    override def interceptCall[ReqT, RespT](
        call: ServerCall[ReqT, RespT],
        headers: Metadata,
        next: ServerCallHandler[ReqT, RespT],
    ): ServerCall.Listener[ReqT] = {
      new Callback(call, headers, next, latestRequestReference.set)
    }
  }

}
