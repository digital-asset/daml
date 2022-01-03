// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.net.{InetAddress, InetSocketAddress}

import akka.pattern.after
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.akka.ServerAdapter
import com.daml.grpc.adapter.utils.implementations.HelloServiceAkkaImplementation
import com.daml.grpc.sampleservice.HelloServiceResponding
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner, TestResourceContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.MetricsInterceptorSpec._
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.hello.HelloServiceGrpc.HelloService
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import com.daml.platform.testing.StreamConsumer
import com.daml.ports.Port
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, Channel, Server, ServerInterceptor, ServerServiceDefinition}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Span}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

final class MetricsInterceptorSpec
    extends AsyncFlatSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with Eventually
    with TestResourceContext {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Second)))

  behavior of "MetricsInterceptor"

  it should "count the number of calls to a given endpoint" in {
    val metrics = new Metrics(new MetricRegistry)
    serverWithMetrics(metrics, new HelloServiceAkkaImplementation).use { channel: Channel =>
      for {
        _ <- Future.sequence(
          (1 to 3).map(reqInt => HelloServiceGrpc.stub(channel).single(HelloRequest(reqInt)))
        )
      } yield {
        eventually {
          metrics.registry.timer("daml.lapi.hello_service.single").getCount shouldBe 3
        }
      }
    }
  }

  it should "time calls to an endpoint" in {
    val metrics = new Metrics(new MetricRegistry)
    serverWithMetrics(metrics, new DelayedHelloService(1.second)).use { channel =>
      for {
        _ <- HelloServiceGrpc.stub(channel).single(HelloRequest(reqInt = 7))
      } yield {
        eventually {
          val metric = metrics.registry.timer("daml.lapi.hello_service.single")
          metric.getCount should be > 0L

          val snapshot = metric.getSnapshot
          val values = Seq(snapshot.getMin, snapshot.getMean.toLong, snapshot.getMax)
          all(values) should (be >= 1.second.toNanos and be <= 3.seconds.toNanos)
        }
      }
    }
  }

  it should "time calls to a streaming endpoint" in {
    val metrics = new Metrics(new MetricRegistry)
    serverWithMetrics(metrics, new DelayedHelloService(1.second)).use { channel =>
      for {
        _ <- new StreamConsumer[HelloResponse](observer =>
          HelloServiceGrpc.stub(channel).serverStreaming(HelloRequest(reqInt = 3), observer)
        ).all()
      } yield {
        eventually {
          val metric = metrics.registry.timer("daml.lapi.hello_service.server_streaming")
          metric.getCount should be > 0L

          val snapshot = metric.getSnapshot
          val values = Seq(snapshot.getMin, snapshot.getMean.toLong, snapshot.getMax)
          all(values) should (be >= 3.seconds.toNanos and be <= 6.seconds.toNanos)
        }
      }
    }
  }
}

object MetricsInterceptorSpec {

  def serverWithMetrics(metrics: Metrics, service: BindableService): ResourceOwner[Channel] =
    for {
      server <- serverOwner(new MetricsInterceptor(metrics), service)
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
              .build()
          server.start()
          server
        })(server => Future(server.shutdown().awaitTermination()))
    }

  private final class DelayedHelloService(delay: FiniteDuration)(implicit
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
  ) extends HelloService
      with HelloServiceResponding
      with BindableService {
    private implicit val executionContext: ExecutionContext = materializer.executionContext

    override def bindService(): ServerServiceDefinition =
      HelloServiceGrpc.bindService(this, executionContext)

    override def single(request: HelloRequest): Future[HelloResponse] =
      after(delay, materializer.system.scheduler)(Future.successful(response(request)))

    override def serverStreaming(
        request: HelloRequest,
        responseObserver: StreamObserver[HelloResponse],
    ): Unit = {
      Source
        .single(request)
        .via(Flow[HelloRequest].mapConcat(responses))
        .mapAsync(1)(response =>
          after(delay, materializer.system.scheduler)(Future.successful(response))
        )
        .runWith(ServerAdapter.toSink(responseObserver))
      ()
    }
  }

}
