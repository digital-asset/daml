// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.health

import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.platform.sandbox.health.HealthServiceSpec._
import io.grpc.health.v1.{HealthCheckRequest, HealthCheckResponse}
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Second, Span}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

object HealthServiceSpec {
  private val request = HealthCheckRequest.getDefaultInstance

  private val servingResponse = HealthCheckResponse
    .newBuilder()
    .setStatus(HealthCheckResponse.ServingStatus.SERVING)
    .build()
}

final class HealthServiceSpec
    extends WordSpec
    with Matchers
    with MockitoSugar
    with Eventually
    with AkkaBeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Second)))

  private implicit val executionContext: ExecutionContext = materializer.executionContext

  "HealthService" should {
    "check the current health" in {
      val responseObserver = mock[StreamObserver[HealthCheckResponse]]
      val service = new HealthService()

      service.check(request, responseObserver)

      val inOrder = Mockito.inOrder(responseObserver)
      inOrder.verify(responseObserver).onNext(servingResponse)
      inOrder.verify(responseObserver).onCompleted()
      inOrder.verifyNoMoreInteractions()
    }

    "observe changes in health" in {
      val responseObserver = mock[StreamObserver[HealthCheckResponse]]
      val service = new HealthService()

      service.watch(request, responseObserver)

      eventually {
        val inOrder = Mockito.inOrder(responseObserver)
        inOrder.verify(responseObserver).onNext(servingResponse)
        inOrder.verifyNoMoreInteractions()
      }
    }

    "stop sending health statuses when the channel is closed" in {
      val responseObserver = mock[StreamObserver[HealthCheckResponse]]
      val service = new HealthService(watchThrottleFrequency = 1.millisecond)

      var isCancelled = false
      service.watch(request, responseObserver, () => isCancelled)

      eventually {
        val inOrder = Mockito.inOrder(responseObserver)
        inOrder.verify(responseObserver).onNext(servingResponse)
        inOrder.verifyNoMoreInteractions()
      }

      isCancelled = true
      when(responseObserver.onCompleted()).thenThrow(new StatusRuntimeException(Status.CANCELLED))

      eventually {
        val inOrder = Mockito.inOrder(responseObserver)
        inOrder.verify(responseObserver).onNext(servingResponse)
        inOrder.verify(responseObserver).onCompleted()
        inOrder.verifyNoMoreInteractions()
      }
    }
  }
}
