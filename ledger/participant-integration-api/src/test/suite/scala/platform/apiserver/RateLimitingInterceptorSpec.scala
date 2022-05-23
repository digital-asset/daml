// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.codahale.metrics.MetricRegistry
import com.daml.grpc.adapter.utils.implementations.HelloServiceAkkaImplementation
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner, TestResourceContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.RateLimitingInterceptorSpec._
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.hello.{HelloRequest, HelloServiceGrpc}
import com.daml.platform.usermanagement.RateLimitingConfig
import com.daml.ports.Port
import io.grpc._
import io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.reflection.v1alpha.{
  ServerReflectionGrpc,
  ServerReflectionRequest,
  ServerReflectionResponse,
}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Second, Span}

import java.net.{InetAddress, InetSocketAddress}
import scala.concurrent.{Future, Promise}

final class RateLimitingInterceptorSpec
    extends AsyncFlatSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with Eventually
    with TestResourceContext {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Second)))

  behavior of "RateLimitingInterceptor"

  it should "limit calls when apiServices executor service is over limit" in {
    val metrics = new Metrics(new MetricRegistry)
    val config = RateLimitingConfig(100)
    withChannel(metrics, new HelloServiceAkkaImplementation, config).use { channel: Channel =>
      val helloService = HelloServiceGrpc.stub(channel)
      val submitted = metrics.registry.meter(
        MetricRegistry.name(metrics.daml.lapi.threadpool.apiServices, "submitted")
      )
      for {
        _ <- helloService.single(HelloRequest(1))
        _ = submitted.mark(config.maxApiServicesQueueSize.toLong + 1)
        exception <- helloService.single(HelloRequest(2)).failed
        _ = submitted.mark(-config.maxApiServicesQueueSize.toLong)
        _ <- helloService.single(HelloRequest(3))
      } yield {
        exception.getMessage should include(metrics.daml.lapi.threadpool.apiServices.toString)
      }
    }
  }

  /** Allowing metadata requests allows grpcurl to be used to debug problems */
  it should "allow metadata requests even when over limit" in {
    val metrics = new Metrics(new MetricRegistry)
    val config = RateLimitingConfig(100)
    metrics.registry
      .meter(MetricRegistry.name(metrics.daml.lapi.threadpool.apiServices, "submitted"))
      .mark(1000) // Over limit

    withChannel(metrics, new HelloServiceAkkaImplementation, config).use { channel: Channel =>
      val methodDescriptor: MethodDescriptor[ServerReflectionRequest, ServerReflectionResponse] =
        ServerReflectionGrpc.getServerReflectionInfoMethod
      val call = channel.newCall(methodDescriptor, CallOptions.DEFAULT)
      val promise = Promise[Status]()
      val listener = new ClientCall.Listener[ServerReflectionResponse]() {
        override def onReady(): Unit = {
          call.request(1)
        }
        override def onClose(status: Status, trailers: Metadata): Unit = {
          promise.success(status)
        }
      }
      call.start(listener, new Metadata())
      val request = ServerReflectionRequest
        .newBuilder()
        .setListServices("services")
        .setHost("localhost")
        .build()
      call.sendMessage(ServerReflectionRequest.newBuilder(request).build())
      call.halfClose()
      promise.future.map(status => status shouldBe Status.OK)
    }
  }

}

object RateLimitingInterceptorSpec {

  def withChannel(
      metrics: Metrics,
      service: BindableService,
      config: RateLimitingConfig,
  ): ResourceOwner[Channel] =
    for {
      server <- serverOwner(new RateLimitingInterceptor(metrics, config), service)
      channel <- GrpcClientResource.owner(Port(server.getPort))
    } yield channel

  private def serverOwner(
      interceptor: ServerInterceptor,
      service: BindableService,
  ): ResourceOwner[Server] =
    new ResourceOwner[Server] {
      def acquire()(implicit context: ResourceContext): Resource[Server] =
        Resource(Future {
          val server =
            NettyServerBuilder
              .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
              .directExecutor()
              .intercept(interceptor)
              .addService(service)
              .addService(ProtoReflectionService.newInstance())
              .build()
          server.start()
          server
        })(server => Future(server.shutdown().awaitTermination()))
    }

}
