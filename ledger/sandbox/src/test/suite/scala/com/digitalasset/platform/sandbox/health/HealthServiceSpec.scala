// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.health

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.digitalasset.platform.sandbox.health.HealthServiceSpec._
import io.grpc.health.v1.{HealthCheckRequest, HealthCheckResponse}
import io.grpc.stub.StreamObserver
import org.mockito.Mockito
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContext

object HealthServiceSpec {
  private val request = HealthCheckRequest.getDefaultInstance

  private val servingResponse = HealthCheckResponse
    .newBuilder()
    .setStatus(HealthCheckResponse.ServingStatus.SERVING)
    .build()
}

class HealthServiceSpec
    extends TestKit(ActorSystem(classOf[HealthServiceSpec].getSimpleName))
    with WordSpecLike
    with Matchers
    with MockitoSugar
    with Eventually
    with BeforeAndAfterAll {

  private[this] implicit val materializer: ActorMaterializer = ActorMaterializer()
  private[this] implicit val executionContext: ExecutionContext = materializer.executionContext

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "HealthService" should {
    "check the current health" in {
      val responseObserver = mock[StreamObserver[HealthCheckResponse]]

      new HealthService().check(request, responseObserver)

      val inOrder = Mockito.inOrder(responseObserver)
      inOrder.verify(responseObserver).onNext(servingResponse)
      inOrder.verify(responseObserver).onCompleted()
      inOrder.verifyNoMoreInteractions()
    }

    "observe changes in health" in {
      val responseObserver = mock[StreamObserver[HealthCheckResponse]]

      new HealthService().watch(request, responseObserver)

      eventually {
        val inOrder = Mockito.inOrder(responseObserver)
        inOrder.verify(responseObserver).onNext(servingResponse)
        inOrder.verifyNoMoreInteractions()
      }
    }
  }
}
