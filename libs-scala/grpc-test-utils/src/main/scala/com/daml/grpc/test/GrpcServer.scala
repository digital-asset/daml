// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.test

import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.services.HealthStatusManager
import io.grpc.{BindableService, Channel, ManagedChannel, Server}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait GrpcServer { this: AsyncFlatSpec =>

  object Services {

    def health: BindableService = new HealthStatusManager().getHealthService

    def reflection: BindableService = ProtoReflectionService.newInstance()

  }

  def withServices(
      service: BindableService,
      services: BindableService*
  )(
      test: Channel => Future[Assertion]
  ): Future[Assertion] = {
    val setup = Future {
      val serverName = InProcessServerBuilder.generateName()
      val serverBuilder = InProcessServerBuilder.forName(serverName).addService(service)
      for (additionalService <- services) {
        serverBuilder.addService(additionalService)
      }
      GrpcServer.Setup(
        server = serverBuilder.build().start(),
        channel = InProcessChannelBuilder.forName(serverName).build(),
      )
    }
    val result = setup.map(_.channel).flatMap(test)
    result.onComplete(_ => setup.map(_.shutdownAndAwaitTerminationFor(5.seconds)))
    result
  }

}

object GrpcServer {

  private final case class Setup(server: Server, channel: ManagedChannel) {
    def shutdownAndAwaitTerminationFor(timeout: FiniteDuration): Unit = {
      server.shutdown()
      channel.shutdown()
      server.awaitTermination()
      channel.awaitTermination(timeout.length, timeout.unit)
      ()
    }
  }

}
