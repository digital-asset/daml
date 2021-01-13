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
import io.grpc.reflection.v1alpha.{
  ServerReflectionGrpc,
  ServerReflectionRequest,
  ServerReflectionResponse,
}
import io.grpc.stub.StreamObserver
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

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

final class ReverseProxySpec extends AsyncFlatSpec with Matchers with GrpcServer with Inside {

  import ReverseProxySpec._
  import Services._

  behavior of "ReverseProxy.create"

  it should "fail if the backend does not support reflection" in withServices(health) { backend =>
    val proxyBuilder = InProcessServerBuilder.forName(InProcessServerBuilder.generateName())
    ReverseProxy
      .create(backend, proxyBuilder, interceptors = Map.empty)
      .failed
      .map(_ shouldBe a[StatusRuntimeException])
  }

  it should "expose the backend's own services" in withServices(health, reflection) { backend =>
    val proxyName = InProcessServerBuilder.generateName()
    val proxyBuilder = InProcessServerBuilder.forName(proxyName)
    ReverseProxy
      .create(backend, proxyBuilder, interceptors = Map.empty)
      .map { proxyServer =>
        proxyServer.start()
        val proxy = InProcessChannelBuilder.forName(proxyName).build()
        getHealthStatus(backend) shouldEqual getHealthStatus(proxy)
        listServices(backend) shouldEqual listServices(proxy)
      }
  }

  it should "correctly set up an interceptor" in withServices(health, reflection) { backend =>
    val proxyName = InProcessServerBuilder.generateName()
    val proxyBuilder = InProcessServerBuilder.forName(proxyName)
    val recorder = new RecordingInterceptor
    ReverseProxy
      .create(backend, proxyBuilder, Map(HealthGrpc.SERVICE_NAME -> Seq(recorder)))
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

  private def listServices(channel: Channel): Iterable[String] = {
    val response = Promise[Iterable[String]]()
    lazy val serverStream: StreamObserver[ServerReflectionRequest] =
      ServerReflectionGrpc
        .newStub(channel)
        .serverReflectionInfo(new StreamObserver[ServerReflectionResponse] {
          override def onNext(value: ServerReflectionResponse): Unit = {
            if (value.hasListServicesResponse) {
              val services = value.getListServicesResponse.getServiceList.asScala.map(_.getName)
              response.trySuccess(services)
            } else {
              response.tryFailure(new IllegalStateException("Received unexpected response type"))
            }
            serverStream.onCompleted()
          }

          override def onError(throwable: Throwable): Unit = {
            val _ = response.tryFailure(throwable)
          }

          override def onCompleted(): Unit = {
            val _ = response.tryFailure(new IllegalStateException("No response received"))
          }
        })
    serverStream.onNext(ServerReflectionRequest.newBuilder().setListServices("").build())
    Await.result(response.future, 5.seconds)
  }

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
