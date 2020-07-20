// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.daml.grpc.sampleservice.implementations.ReferenceImplementation
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.GrpcServerSpec._
import com.daml.platform.hello.{HelloRequest, HelloServiceGrpc}
import com.daml.ports.{FreePort, Port}
import com.daml.resources.{Resource, ResourceOwner}
import com.google.protobuf.ByteString
import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.{ExecutionContext, Future}

final class GrpcServerSpec extends AsyncWordSpec with Matchers {
  "a GRPC server" should {
    "handle a request to a valid service" in {
      resources().use { channel =>
        val helloService = HelloServiceGrpc.stub(channel)
        for {
          response <- helloService.single(HelloRequest(7))
        } yield {
          response.respInt shouldBe 14
        }
      }
    }

    "fail with a nice exception" in {
      resources().use { channel =>
        val helloService = HelloServiceGrpc.stub(channel)
        for {
          exception <- helloService
            .fails(HelloRequest(7, ByteString.copyFromUtf8("This is some text.")))
            .failed
        } yield {
          exception.getMessage shouldBe "INTERNAL: This is some text."
        }
      }
    }
  }
}

object GrpcServerSpec {

  private def resources(): ResourceOwner[ManagedChannel] = {
    for {
      eventLoopGroups <- new ServerEventLoopGroups.Owner(
        classOf[GrpcServerSpec].getSimpleName,
        workerParallelism = sys.runtime.availableProcessors(),
        bossParallelism = 1,
      )
      port = FreePort.find()
      _ <- new GrpcServer.Owner(
        address = None,
        desiredPort = port,
        maxInboundMessageSize = 4 * 1024 * 1024,
        metrics = new Metrics(new MetricRegistry),
        eventLoopGroups = eventLoopGroups,
        services = Seq(new ReferenceImplementation),
      )
      channel <- new ChannelOwner(port)
    } yield channel
  }

  final class ChannelOwner(port: Port) extends ResourceOwner[ManagedChannel] {
    override def acquire()(implicit executionContext: ExecutionContext): Resource[ManagedChannel] =
      Resource(
        Future {
          NettyChannelBuilder
            .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, port.value))
            .usePlaintext()
            .build()
        }
      )(channel =>
        Future {
          channel.shutdown().awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
          ()
      })
  }

}
