// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.reflection

import java.util.concurrent.TimeUnit

import io.grpc.health.v1.HealthGrpc
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.reflection.v1alpha.ServerReflectionGrpc
import io.grpc.services.HealthStatusManager
import io.grpc.{BindableService, Channel, StatusRuntimeException}
import org.scalatest.Assertion
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

final class ServerReflectionClientSpec extends AsyncFlatSpec with Matchers with ScalaFutures {

  import ServerReflectionClientSpec._

  behavior of "getAllServices"

  it should "fail if reflection is not supported" in withServices(health) { channel =>
    val stub = ServerReflectionGrpc.newStub(channel)
    val client = new ServerReflectionClient(stub)
    client.getAllServices().failed.futureValue shouldBe a[StatusRuntimeException]
  }

  it should "show all if reflection is supported" in withServices(health, reflection) { channel =>
    val expected = Vector(healthDescriptor, reflectionDescriptor)
    val stub = ServerReflectionGrpc.newStub(channel)
    val client = new ServerReflectionClient(stub)
    client.getAllServices().map(_ should contain theSameElementsAs expected)
  }

}

object ServerReflectionClientSpec {

  private def health: BindableService = new HealthStatusManager().getHealthService
  private val healthDescriptor =
    ServiceDescriptorInfo(
      fullServiceName = HealthGrpc.SERVICE_NAME,
      methods = Set(
        MethodDescriptorInfo(HealthGrpc.getCheckMethod),
        MethodDescriptorInfo(HealthGrpc.getWatchMethod),
      ),
    )

  private def reflection: BindableService = ProtoReflectionService.newInstance()
  private val reflectionDescriptor =
    ServiceDescriptorInfo(
      fullServiceName = ServerReflectionGrpc.SERVICE_NAME,
      methods = Set(
        MethodDescriptorInfo(ServerReflectionGrpc.getServerReflectionInfoMethod)
      ),
    )

  private def withServices(service: BindableService, services: BindableService*)(
      f: Channel => Future[Assertion]
  ): Future[Assertion] = {
    val serverName = InProcessServerBuilder.generateName()
    val serverBuilder = InProcessServerBuilder.forName(serverName).addService(service)
    for (additionalService <- services) {
      serverBuilder.addService(additionalService)
    }
    val server = serverBuilder.build()
    val channel = InProcessChannelBuilder.forName(serverName).build()
    try {
      server.start()
      f(channel)
    } finally {
      server.shutdown()
      val _ = server.awaitTermination(5, TimeUnit.SECONDS)
    }
  }

}
