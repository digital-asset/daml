// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.net.{InetAddress, InetSocketAddress}

import akka.pattern.after
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import com.codahale.metrics.MetricRegistry
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.grpc.adapter.server.akka.ServerAdapter
import com.digitalasset.grpc.adapter.utils.implementations.AkkaImplementation
import com.digitalasset.grpc.sampleservice.Responding
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.digitalasset.platform.apiserver.MetricsInterceptorSpec._
import com.digitalasset.platform.hello.HelloServiceGrpc.HelloService
import com.digitalasset.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import com.digitalasset.platform.sandbox.services.GrpcClientResource
import com.digitalasset.platform.testing.StreamConsumer
import com.digitalasset.ports.Port
import com.digitalasset.resources.{Resource, ResourceOwner}
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, Server, ServerInterceptor, ServerServiceDefinition}
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

final class MetricsInterceptorSpec extends AsyncFlatSpec with AkkaBeforeAndAfterAll with Matchers {

  behavior of "MetricsInterceptor.camelCaseToSnakeCase"

  import MetricsInterceptor.camelCaseToSnakeCase

  it should "leave an empty string unchanged" in {
    camelCaseToSnakeCase("") shouldBe ""
  }

  it should "leave a snake_cased string unchanged" in {
    camelCaseToSnakeCase("snake_cased") shouldBe "snake_cased"
  }

  it should "remove the capitalization of the first letter" in {
    camelCaseToSnakeCase("Camel") shouldBe "camel"
  }

  it should "turn a single capital letter into a an underscore followed by a lower case letter" in {
    camelCaseToSnakeCase("CamelCase") shouldBe "camel_case"
    camelCaseToSnakeCase("camelCase") shouldBe "camel_case"
  }

  it should "keep acronyms together and change their capitalization as a single unit" in {
    camelCaseToSnakeCase("DAML") shouldBe "daml"
    camelCaseToSnakeCase("DAMLFactory") shouldBe "daml_factory"
    camelCaseToSnakeCase("AbstractDAML") shouldBe "abstract_daml"
    camelCaseToSnakeCase("AbstractDAMLFactory") shouldBe "abstract_daml_factory"
    camelCaseToSnakeCase("AbstractDAMLProxyJVMFactory") shouldBe "abstract_daml_proxy_jvm_factory"
  }

  it should "treat single letter words intelligently" in {
    camelCaseToSnakeCase("ATeam") shouldBe "a_team"
    camelCaseToSnakeCase("TeamA") shouldBe "team_a"
    camelCaseToSnakeCase("BustAMove") shouldBe "bust_a_move"

    // the following is mostly to document a reasonable short-coming:
    // a single letter word followed by an acronym will be detected as a single acronym
    camelCaseToSnakeCase("AJVMHeap") shouldBe "ajvm_heap"
  }

  behavior of "MetricsInterceptor.nameFor"

  import MetricsInterceptor.nameFor

  it should "produce the expected name for a selection of services" in {
    nameFor(CommandServiceGrpc.METHOD_SUBMIT_AND_WAIT.getFullMethodName) shouldBe "daml.lapi.command_service.submit_and_wait"
    nameFor(CommandSubmissionServiceGrpc.METHOD_SUBMIT.getFullMethodName) shouldBe "daml.lapi.command_submission_service.submit"
    nameFor(ActiveContractsServiceGrpc.METHOD_GET_ACTIVE_CONTRACTS.getFullMethodName) shouldBe "daml.lapi.active_contracts_service.get_active_contracts"
  }

  behavior of "MetricsInterceptor"

  it should "count the number of calls to a given endpoint" in {
    val metrics = new MetricRegistry
    val interceptor = new MetricsInterceptor(metrics)
    val connection = for {
      server <- serverOwner(interceptor, new AkkaImplementation)
      channel <- GrpcClientResource.owner(Port(server.getPort))
    } yield channel
    connection.use { channel =>
      for {
        _ <- Future.sequence(
          (1 to 3).map(reqInt => HelloServiceGrpc.stub(channel).single(HelloRequest(reqInt))))
      } yield {
        metrics.timer("daml.lapi.hello_service.single").getCount shouldBe 3
      }
    }
  }

  it should "time calls to a given endpoint" in {
    val metrics = new MetricRegistry
    val interceptor = new MetricsInterceptor(metrics)
    val connection = for {
      server <- serverOwner(interceptor, new DelayedAkkaImplementation(1.second))
      channel <- GrpcClientResource.owner(Port(server.getPort))
    } yield channel
    connection.use { channel =>
      for {
        _ <- new StreamConsumer[HelloResponse](observer =>
          HelloServiceGrpc.stub(channel).serverStreaming(HelloRequest(reqInt = 3), observer)).all()
      } yield {
        val snapshot = metrics.timer("daml.lapi.hello_service.server_streaming").getSnapshot
        all(Seq(snapshot.getMin, snapshot.getMean.toLong, snapshot.getMax)) should (be >= 3.seconds.toNanos and be <= 6.seconds.toNanos)
      }
    }
  }
}

object MetricsInterceptorSpec {

  private def serverOwner(
      interceptor: ServerInterceptor,
      service: BindableService,
  ): ResourceOwner[Server] =
    new ResourceOwner[Server] {
      def acquire()(implicit executionContext: ExecutionContext): Resource[Server] =
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

  private final class DelayedAkkaImplementation(delay: FiniteDuration)(
      implicit executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
  ) extends HelloService
      with Responding
      with BindableService {

    override def bindService(): ServerServiceDefinition =
      HelloServiceGrpc.bindService(this, materializer.executionContext)

    override def serverStreaming(
        request: HelloRequest,
        responseObserver: StreamObserver[HelloResponse],
    ): Unit = {
      Source
        .single(request)
        .via(Flow[HelloRequest].mapConcat(responses))
        .mapAsync(1)(response =>
          after(delay, materializer.system.scheduler)(Future.successful(response))(
            materializer.executionContext))
        .runWith(ServerAdapter.toSink(responseObserver))
      ()
    }
  }

}
