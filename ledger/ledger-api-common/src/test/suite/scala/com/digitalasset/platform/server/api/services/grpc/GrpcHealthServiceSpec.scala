// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import com.daml.grpc.GrpcException
import com.daml.grpc.adapter.server.rs.MockServerCallStreamObserver
import com.daml.ledger.api.health._
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.platform.server.api.services.grpc.GrpcHealthService._
import com.daml.platform.server.api.services.grpc.GrpcHealthServiceSpec._
import io.grpc.health.v1.health.{HealthCheckRequest, HealthCheckResponse}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Span}
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.duration.DurationInt

final class GrpcHealthServiceSpec
    extends AsyncWordSpec
    with Matchers
    with Eventually
    with AkkaBeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Second)))

  "HealthService" should {
    "report SERVING if there are no health checks" in {
      val service = new GrpcHealthService(new HealthChecks)

      for {
        response <- service.check(HealthCheckRequest())
      } yield {
        response should be(servingResponse)
      }
    }

    "report SERVING if there is one healthy check" in {
      val service = new GrpcHealthService(new HealthChecks("component" -> healthyComponent))

      for {
        response <- service.check(HealthCheckRequest())
      } yield {
        response should be(servingResponse)
      }
    }

    "report NOT_SERVING if there is one unhealthy check" in {
      val service = new GrpcHealthService(new HealthChecks("component" -> unhealthyComponent))

      for {
        response <- service.check(HealthCheckRequest())
      } yield {
        response should be(notServingResponse)
      }
    }

    "report SERVING if all checks are healthy" in {
      val service = new GrpcHealthService(
        new HealthChecks(
          "component A" -> healthyComponent,
          "component B" -> healthyComponent,
          "component C" -> healthyComponent,
        ))

      for {
        response <- service.check(HealthCheckRequest())
      } yield {
        response should be(servingResponse)
      }
    }

    "report NOT_SERVING if a single check is unhealthy" in {
      val service = new GrpcHealthService(
        new HealthChecks(
          "component A" -> healthyComponent,
          "component B" -> unhealthyComponent,
          "component C" -> healthyComponent,
        ))

      for {
        response <- service.check(HealthCheckRequest())
      } yield {
        response should be(notServingResponse)
      }
    }

    "report SERVING when querying a single, healthy component" in {
      val service = new GrpcHealthService(new HealthChecks("component" -> healthyComponent))

      for {
        response <- service.check(HealthCheckRequest("component"))
      } yield {
        response should be(servingResponse)
      }
    }

    "report NOT_SERVING when querying a single, unhealthy component" in {
      val service = new GrpcHealthService(new HealthChecks("component" -> unhealthyComponent))

      for {
        response <- service.check(HealthCheckRequest("component"))
      } yield {
        response should be(notServingResponse)
      }
    }

    "report SERVING when querying a healthy component alongside other, unhealthy components" in {
      val service = new GrpcHealthService(
        new HealthChecks(
          "component A" -> healthyComponent,
          "component B" -> healthyComponent,
          "component C" -> unhealthyComponent,
        ))

      for {
        response <- service.check(HealthCheckRequest("component B"))
      } yield {
        response should be(servingResponse)
      }
    }

    "report NOT_SERVING when querying an unhealthy component alongside other, healthy components" in {
      val service = new GrpcHealthService(
        new HealthChecks(
          "component A" -> unhealthyComponent,
          "component B" -> healthyComponent,
          "component C" -> healthyComponent,
        ))

      for {
        response <- service.check(HealthCheckRequest("component A"))
      } yield {
        response should be(notServingResponse)
      }
    }

    "observe changes in health" in {
      val responseObserver = new MockServerCallStreamObserver[HealthCheckResponse]

      var componentAHealth: HealthStatus = Healthy
      var componentBHealth: HealthStatus = Healthy
      var componentCHealth: HealthStatus = Healthy
      val service = new GrpcHealthService(
        new HealthChecks(
          "component A" -> compnentWithHealthBackedBy(() => componentAHealth),
          "component B" -> compnentWithHealthBackedBy(() => componentBHealth),
          "component C" -> compnentWithHealthBackedBy(() => componentCHealth),
        ),
        maximumWatchFrequency = 1.millisecond,
      )

      service.watch(HealthCheckRequest(), responseObserver)
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

      // this won't emit a new response, because the overall health of the system didn't change.
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
          "component A" -> compnentWithHealthBackedBy(() => componentAHealth),
          "component B" -> compnentWithHealthBackedBy(() => componentBHealth),
          "component C" -> compnentWithHealthBackedBy(() => componentCHealth),
        ),
        maximumWatchFrequency = 1.millisecond,
      )

      service.watch(HealthCheckRequest("component C"), responseObserver)
      responseObserver.demandResponse(count = 3)

      eventually {
        responseObserver.elements should be(Vector(servingResponse))
      }

      // this component won't affect the health of component C
      componentBHealth = Unhealthy
      eventually {
        responseObserver.elements should be(Vector(servingResponse))
      }

      // this component won't affect the health of component C
      componentBHealth = Healthy
      eventually {
        responseObserver.elements should be(Vector(servingResponse))
      }

      // this component won't affect the health of component C
      componentAHealth = Unhealthy
      eventually {
        responseObserver.elements should be(Vector(servingResponse))
      }

      componentCHealth = Unhealthy
      eventually {
        responseObserver.elements should be(Vector(servingResponse, notServingResponse))
      }

      componentCHealth = Healthy
      eventually {
        responseObserver.elements should be(
          Vector(servingResponse, notServingResponse, servingResponse))
      }

      // this component won't affect the health of component C
      componentAHealth = Healthy
      eventually {
        responseObserver.elements should be(
          Vector(servingResponse, notServingResponse, servingResponse))
      }
    }
  }

  "fail gracefully when a non-existent component is checked" in {
    val service = new GrpcHealthService(new HealthChecks("component" -> unhealthyComponent))

    for {
      throwable <- service.check(HealthCheckRequest("another component")).failed
    } yield {
      throwable match {
        case GrpcException.NOT_FOUND() =>
          succeed
        case _ =>
          fail("Expected a NOT_FOUND error.")
      }
    }
  }

  "fail gracefully when a non-existent component is watched" in {
    val responseObserver = new MockServerCallStreamObserver[HealthCheckResponse]
    val service = new GrpcHealthService(new HealthChecks("component" -> unhealthyComponent))

    service.watch(HealthCheckRequest("another component"), responseObserver)
    responseObserver.demandResponse()

    for {
      throwable <- responseObserver.completionFuture.failed
    } yield {
      throwable match {
        case GrpcException.NOT_FOUND() =>
          succeed
        case _ =>
          fail("Expected a NOT_FOUND error.")
      }
    }
  }
}

object GrpcHealthServiceSpec {
  private val healthyComponent: ReportsHealth = () => Healthy

  private val unhealthyComponent: ReportsHealth = () => Unhealthy

  private def compnentWithHealthBackedBy(fetchCurrentHealth: () => HealthStatus): ReportsHealth =
    () => fetchCurrentHealth()
}
