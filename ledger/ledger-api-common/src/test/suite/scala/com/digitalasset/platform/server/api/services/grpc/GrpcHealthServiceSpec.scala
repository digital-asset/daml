// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.grpc

import com.digitalasset.grpc.GrpcException
import com.digitalasset.grpc.adapter.server.rs.MockServerCallStreamObserver
import com.digitalasset.ledger.api.health._
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.platform.server.api.services.grpc.GrpcHealthService._
import com.digitalasset.platform.server.api.services.grpc.GrpcHealthServiceSpec._
import io.grpc.health.v1.health.{HealthCheckRequest, HealthCheckResponse}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Span}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

final class GrpcHealthServiceSpec
    extends WordSpec
    with Matchers
    with Eventually
    with AkkaBeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Second)))

  private implicit val executionContext: ExecutionContext = materializer.executionContext

  "HealthService" should {
    "report SERVING if there are no health checks" in {
      val service = new GrpcHealthService(HealthChecks.empty)

      val response = Await.result(service.check(allServicesRequest), patienceConfig.timeout)

      response should be(servingResponse)
    }

    "report SERVING if there is one healthy check" in {
      val service = new GrpcHealthService(new HealthChecks("component" -> healthyComponent))

      val response = Await.result(service.check(allServicesRequest), patienceConfig.timeout)

      response should be(servingResponse)
    }

    "report NOT_SERVING if there is one unhealthy check" in {
      val service = new GrpcHealthService(new HealthChecks("component" -> unhealthyComponent))

      val response = Await.result(service.check(allServicesRequest), patienceConfig.timeout)

      response should be(notServingResponse)
    }

    "report SERVING if all checks are healthy" in {
      val service = new GrpcHealthService(
        new HealthChecks(
          "component A" -> healthyComponent,
          "component B" -> healthyComponent,
          "component C" -> healthyComponent,
        ))

      val response = Await.result(service.check(allServicesRequest), patienceConfig.timeout)

      response should be(servingResponse)
    }

    "report NOT_SERVING if a single check is unhealthy" in {
      val service = new GrpcHealthService(
        new HealthChecks(
          "component A" -> healthyComponent,
          "component B" -> unhealthyComponent,
          "component C" -> healthyComponent,
        ))

      val response = Await.result(service.check(allServicesRequest), patienceConfig.timeout)

      response should be(notServingResponse)
    }

    "report SERVING when querying a single, healthy component" in {
      val service = new GrpcHealthService(new HealthChecks("component" -> healthyComponent))

      val response =
        Await.result(service.check(serviceRequestFor("component")), patienceConfig.timeout)

      response should be(servingResponse)
    }

    "report NOT_SERVING when querying a single, unhealthy component" in {
      val service = new GrpcHealthService(new HealthChecks("component" -> unhealthyComponent))

      val response =
        Await.result(service.check(serviceRequestFor("component")), patienceConfig.timeout)

      response should be(notServingResponse)
    }

    "report SERVING when querying a healthy component alongside other, unhealthy components" in {
      val service = new GrpcHealthService(
        new HealthChecks(
          "component A" -> healthyComponent,
          "component B" -> healthyComponent,
          "component C" -> unhealthyComponent,
        ))

      val response =
        Await.result(service.check(serviceRequestFor("component B")), patienceConfig.timeout)

      response should be(servingResponse)
    }

    "report NOT_SERVING when querying an unhealthy component alongside other, healthy components" in {
      val service = new GrpcHealthService(
        new HealthChecks(
          "component A" -> unhealthyComponent,
          "component B" -> healthyComponent,
          "component C" -> healthyComponent,
        ))

      val response =
        Await.result(service.check(serviceRequestFor("component A")), patienceConfig.timeout)

      response should be(notServingResponse)
    }

    "observe changes in health" in {
      val responseObserver = new MockServerCallStreamObserver[HealthCheckResponse]

      var componentAHealth: HealthStatus = Healthy
      var componentBHealth: HealthStatus = Healthy
      var componentCHealth: HealthStatus = Healthy
      val service = new GrpcHealthService(
        new HealthChecks(
          "component A" -> new StubReporter(componentAHealth),
          "component B" -> new StubReporter(componentBHealth),
          "component C" -> new StubReporter(componentCHealth),
        ),
        maximumWatchFrequency = 1.millisecond,
      )

      service.watch(allServicesRequest, responseObserver)
      responseObserver.demandResponse(count = 5)

      eventually {
        responseObserver.elements should be(Vector(servingResponse))
      }

      componentBHealth = Unhealthy
      eventually {
        responseObserver.elements should be(Vector(servingResponse, notServingResponse))
      }

      componentBHealth = Healthy
      eventually {
        responseObserver.elements should be(
          Vector(servingResponse, notServingResponse, servingResponse))
      }

      componentAHealth = Unhealthy
      eventually {
        responseObserver.elements should be(
          Vector(servingResponse, notServingResponse, servingResponse, notServingResponse))
      }

      componentCHealth = Unhealthy
      eventually {
        responseObserver.elements should be(
          Vector(servingResponse, notServingResponse, servingResponse, notServingResponse))
      }

      componentCHealth = Healthy
      componentAHealth = Healthy
      eventually {
        responseObserver.elements should be(
          Vector(
            servingResponse,
            notServingResponse,
            servingResponse,
            notServingResponse,
            servingResponse,
          ))
      }
    }

    "observe changes in a single component's health" in {
      val responseObserver = new MockServerCallStreamObserver[HealthCheckResponse]

      var componentAHealth: HealthStatus = Healthy
      var componentBHealth: HealthStatus = Healthy
      var componentCHealth: HealthStatus = Healthy
      val service = new GrpcHealthService(
        new HealthChecks(
          "component A" -> new StubReporter(componentAHealth),
          "component B" -> new StubReporter(componentBHealth),
          "component C" -> new StubReporter(componentCHealth),
        ),
        maximumWatchFrequency = 1.millisecond,
      )

      service.watch(serviceRequestFor("component C"), responseObserver)
      responseObserver.demandResponse(count = 3)

      eventually {
        responseObserver.elements should be(Vector(servingResponse))
      }

      componentBHealth = Unhealthy
      eventually {
        responseObserver.elements should be(Vector(servingResponse))
      }

      componentBHealth = Healthy
      eventually {
        responseObserver.elements should be(Vector(servingResponse))
      }

      componentAHealth = Unhealthy
      eventually {
        responseObserver.elements should be(Vector(servingResponse))
      }

      componentCHealth = Unhealthy
      eventually {
        responseObserver.elements should be(Vector(servingResponse, notServingResponse))
      }

      componentCHealth = Healthy
      componentAHealth = Healthy
      eventually {
        responseObserver.elements should be(
          Vector(servingResponse, notServingResponse, servingResponse))
      }
    }
  }

  "fail gracefully when a non-existent component is checked" in {
    val service = new GrpcHealthService(new HealthChecks("component" -> unhealthyComponent))

    val response = service.check(serviceRequestFor("another component"))
    try {
      Await.result(response, patienceConfig.timeout)
      fail("Expected a NOT_FOUND error, but got a successful result.")
    } catch {
      case GrpcException.NOT_FOUND() =>
        succeed
    }
  }

  "fail gracefully when a non-existent component is watched" in {
    val responseObserver = new MockServerCallStreamObserver[HealthCheckResponse]
    val service = new GrpcHealthService(new HealthChecks("component" -> unhealthyComponent))

    service.watch(serviceRequestFor("another component"), responseObserver)
    responseObserver.demandResponse()

    try {
      Await.result(responseObserver.completionFuture, patienceConfig.timeout)
      fail("Expected a NOT_FOUND error, but got a successful result.")
    } catch {
      case GrpcException.NOT_FOUND() =>
        succeed
    }
  }
}

object GrpcHealthServiceSpec {
  private val allServicesRequest = HealthCheckRequest()

  private def serviceRequestFor(componentName: String) = HealthCheckRequest(service = componentName)

  private val healthyComponent: ReportsHealth = new StubReporter(Healthy)

  private val unhealthyComponent: ReportsHealth = new StubReporter(Unhealthy)

  private class StubReporter(_currentHealth: => HealthStatus) extends ReportsHealth {
    override def currentHealth(): HealthStatus = _currentHealth
  }

}
