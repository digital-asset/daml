// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.test

import com.daml.resources.grpc.GrpcResourceOwnerFactories as Resources
import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import io.grpc.health.v1.{HealthCheckRequest, HealthGrpc}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.protobuf.services.{HealthStatusManager, ProtoReflectionServiceV1}
import io.grpc.reflection.v1alpha.{
  ServerReflectionGrpc,
  ServerReflectionRequest,
  ServerReflectionResponse,
}
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, Channel, ClientInterceptor}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.jdk.CollectionConverters.*

trait GrpcServer { this: AsyncFlatSpec =>

  object Services {

    object Health {

      object Requests {
        val Check: HealthCheckRequest =
          HealthCheckRequest.newBuilder().build()
      }

      val Name: String = HealthGrpc.SERVICE_NAME

      def newInstance: BindableService = new HealthStatusManager().getHealthService

      def getHealthStatus(channel: Channel, interceptors: ClientInterceptor*): ServingStatus =
        HealthGrpc
          .newBlockingStub(channel)
          .withInterceptors(interceptors*)
          .check(Requests.Check)
          .getStatus

    }

    object Reflection {

      object Requests {
        val ListServices: ServerReflectionRequest =
          ServerReflectionRequest.newBuilder().setListServices("").build()
      }

      val Name: String = ServerReflectionGrpc.SERVICE_NAME

      def newInstance: BindableService = ProtoReflectionServiceV1.newInstance()

      def listServices(channel: Channel, interceptors: ClientInterceptor*): Iterable[String] = {
        val response = Promise[Iterable[String]]()
        lazy val serverStream: StreamObserver[ServerReflectionRequest] =
          ServerReflectionGrpc
            .newStub(channel)
            .withInterceptors(interceptors*)
            .serverReflectionInfo(new StreamObserver[ServerReflectionResponse] {
              override def onNext(value: ServerReflectionResponse): Unit = {
                if (value.hasListServicesResponse) {
                  val services = value.getListServicesResponse.getServiceList.asScala.map(_.getName)
                  response.trySuccess(services)
                } else {
                  response
                    .tryFailure(new IllegalStateException("Received unexpected response type"))
                }
                serverStream.onCompleted()
              }

              override def onError(throwable: Throwable): Unit = {
                val _ = response.tryFailure(throwable)
              }

              override def onCompleted(): Unit = {
                val _ = response.tryFailure(new IllegalStateException("No response received"))
              }
            })
        serverStream.onNext(Requests.ListServices)
        Await.result(response.future, 5.seconds)
      }

    }

  }

  def withServices(
      service: BindableService,
      services: BindableService*
  )(
      test: Channel => Future[Assertion]
  ): Future[Assertion] = {
    val serverName = InProcessServerBuilder.generateName()
    val serverBuilder = InProcessServerBuilder.forName(serverName).addService(service)
    for (additionalService <- services) {
      serverBuilder.addService(additionalService)
    }
    val channelBuilder = InProcessChannelBuilder.forName(serverName)
    val channelOwner =
      for {
        _ <- Resources.forServer(serverBuilder, 5.seconds)
        channel <- Resources.forChannel(channelBuilder, 5.seconds)
      } yield channel

    channelOwner.use(test)
  }

}
