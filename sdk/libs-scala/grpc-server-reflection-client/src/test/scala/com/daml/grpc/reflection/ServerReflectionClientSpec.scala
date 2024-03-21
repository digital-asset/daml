// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.reflection

import com.daml.grpc.test.GrpcServer
import io.grpc.StatusRuntimeException
import io.grpc.health.v1.HealthGrpc
import io.grpc.reflection.v1alpha.ServerReflectionGrpc
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

final class ServerReflectionClientSpec extends AsyncFlatSpec with Matchers with GrpcServer {

  import ServerReflectionClientSpec._
  import Services._

  behavior of "getAllServices"

  it should "fail if reflection is not supported" in withServices(Health.newInstance) { channel =>
    val stub = ServerReflectionGrpc.newStub(channel)
    val client = new ServerReflectionClient(stub)
    client.getAllServices().failed.map(_ shouldBe a[StatusRuntimeException])
  }

  it should "show all if reflection is supported" in withServices(
    Health.newInstance,
    Reflection.newInstance,
  ) { channel =>
    val expected = Set(healthDescriptor, reflectionDescriptor)
    val stub = ServerReflectionGrpc.newStub(channel)
    val client = new ServerReflectionClient(stub)
    client.getAllServices().map(_ should contain theSameElementsAs expected)
  }

}

object ServerReflectionClientSpec {

  private val healthDescriptor =
    ServiceDescriptorInfo(
      fullServiceName = HealthGrpc.SERVICE_NAME,
      methods = Set(
        MethodDescriptorInfo(HealthGrpc.getCheckMethod),
        MethodDescriptorInfo(HealthGrpc.getWatchMethod),
      ),
    )

  private val reflectionDescriptor =
    ServiceDescriptorInfo(
      fullServiceName = ServerReflectionGrpc.SERVICE_NAME,
      methods = Set(MethodDescriptorInfo(ServerReflectionGrpc.getServerReflectionInfoMethod)),
    )

}
