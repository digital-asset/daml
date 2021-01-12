// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc

import java.util.concurrent.atomic.AtomicReference

import com.daml.grpc.test.GrpcServer
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import io.grpc.health.v1.{HealthCheckRequest, HealthGrpc}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{
  Channel,
  Metadata,
  ServerCall,
  ServerCallHandler,
  ServerInterceptor,
  StatusRuntimeException,
}
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

final class ReverseProxySpec extends AsyncFlatSpec with Matchers with GrpcServer with Inside {

  import ReverseProxySpec._
  import Services._

  behavior of "ReverseProxy.create"

  it should "fail if the backend does not support reflection" in withServices(health) { backend =>
    val proxyBuilder = InProcessServerBuilder.forName(InProcessServerBuilder.generateName())
    ReverseProxy
      .create(backend, proxyBuilder)
      .failed
      .map(_ shouldBe a[StatusRuntimeException])
  }

  it should "expose the backend's own services" in withServices(health, reflection) { backend =>
    val proxyName = InProcessServerBuilder.generateName()
    val proxyBuilder = InProcessServerBuilder.forName(proxyName)
    ReverseProxy
      .create(backend, proxyBuilder)
      .map { proxyServer =>
        proxyServer.start()
        val proxy = InProcessChannelBuilder.forName(proxyName).build()
        getHealthStatus(backend) shouldEqual getHealthStatus(proxy)
      }
  }

  it should "correctly set up an interceptor" in withServices(health, reflection) { backend =>
    val proxyName = InProcessServerBuilder.generateName()
    val proxyBuilder = InProcessServerBuilder.forName(proxyName)
    val recorder = new RecordingInterceptor
    ReverseProxy
      .create(backend, proxyBuilder, HealthGrpc.SERVICE_NAME -> Seq(recorder))
      .map { proxyServer =>
        proxyServer.start()
        val proxy = InProcessChannelBuilder.forName(proxyName).build()
        getHealthStatus(backend)
        recorder.latestRequest() shouldBe empty
        getHealthStatus(proxy)
        inside(recorder.latestRequest()) { case Some(request: Array[Byte]) =>
          HealthCheckRequest.parseFrom(request)
          succeed
        }
      }
  }

}

object ReverseProxySpec {

  private def getHealthStatus(channel: Channel): ServingStatus =
    HealthGrpc
      .newBlockingStub(channel)
      .check(HealthCheckRequest.newBuilder().build())
      .getStatus

  final class Forward[ReqT, RespT](call: ServerCall[ReqT, RespT])
      extends SimpleForwardingServerCall[ReqT, RespT](call) {
    override def sendMessage(message: RespT): Unit = {
      super.sendMessage(message)
    }
  }

  final class Callback[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
      callback: ReqT => Unit,
  ) extends SimpleForwardingServerCallListener[ReqT](next.startCall(new Forward(call), headers)) {
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
