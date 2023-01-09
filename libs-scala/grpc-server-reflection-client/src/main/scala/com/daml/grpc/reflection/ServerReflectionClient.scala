// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.reflection

import io.grpc.reflection.v1alpha.ServerReflectionGrpc.ServerReflectionStub
import io.grpc.reflection.v1alpha.ServerReflectionRequest
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
