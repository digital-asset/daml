// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.reflection

import io.grpc.reflection.v1.ServerReflectionGrpc.ServerReflectionStub
import io.grpc.reflection.v1.ServerReflectionRequest
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

final class ServerReflectionClient(stub: ServerReflectionStub) {

  def getAllServices(): Future[Set[ServiceDescriptorInfo]] = {

    lazy val serviceDescriptorInfo: ServiceDescriptorInfoObserver =
      new ServiceDescriptorInfoObserver(serverReflectionStream)

    lazy val serverReflectionStream: StreamObserver[ServerReflectionRequest] =
      stub.serverReflectionInfo(serviceDescriptorInfo)

    serviceDescriptorInfo.result

  }

}
