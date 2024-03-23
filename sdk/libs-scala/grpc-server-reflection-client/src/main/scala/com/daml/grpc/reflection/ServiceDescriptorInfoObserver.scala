// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.reflection

import java.util.concurrent.atomic.AtomicInteger

import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import io.grpc.Status
import io.grpc.reflection.v1alpha.{ServerReflectionRequest, ServerReflectionResponse}
import io.grpc.stub.StreamObserver

import scala.jdk.CollectionConverters._
import scala.concurrent.{Future, Promise}
import scala.util.Success

private[reflection] final class ServiceDescriptorInfoObserver(
    serverReflectionStream: => StreamObserver[ServerReflectionRequest]
) extends StreamObserver[ServerReflectionResponse] {

  private val builder = Set.newBuilder[ServiceDescriptorInfo]
  private val promise = Promise[Set[ServiceDescriptorInfo]]()
  private val servicesLeft = new AtomicInteger(0)

  lazy val result: Future[Set[ServiceDescriptorInfo]] = {
    serverReflectionStream.onNext(ServerReflectionRequests.ListServices)
    promise.future
  }

  override def onNext(response: ServerReflectionResponse): Unit = {
    if (response.hasListServicesResponse) {
      servicesLeft.set(response.getListServicesResponse.getServiceCount)
      for (service <- response.getListServicesResponse.getServiceList.asScala) {
        serverReflectionStream.onNext(ServerReflectionRequests.fileContaining(service.getName))
      }
    } else if (response.hasFileDescriptorResponse) {
      for (bytes <- response.getFileDescriptorResponse.getFileDescriptorProtoList.asScala) {
        val fileDescriptorProto = FileDescriptorProto.parseFrom(bytes)
        for (service <- fileDescriptorProto.getServiceList.asScala) {
          builder +=
            ServiceDescriptorInfo(
              packageName = fileDescriptorProto.getPackage,
              serviceName = service.getName,
              methods = service.getMethodList.asScala,
            )
        }
      }
      if (servicesLeft.decrementAndGet() < 1) {
        serverReflectionStream.onCompleted()
      }
    } else if (response.hasErrorResponse) {
      val error = response.getErrorResponse
      val throwable = Status
        .fromCodeValue(error.getErrorCode)
        .withDescription(error.getErrorMessage)
        .asRuntimeException()
      serverReflectionStream.onError(throwable)
    }
  }

  override def onError(throwable: Throwable): Unit = {
    val _ = promise.tryFailure(throwable)
  }

  override def onCompleted(): Unit = {
    val _ = promise.tryComplete(Success(builder.result()))
  }
}
