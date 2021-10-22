// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.grpc.GrpcException
import com.daml.grpc.adapter.server.rs.MockServerCallStreamObserver
import com.daml.ledger.api.health._
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.logging.LoggingContext
import com.daml.platform.server.api.services.grpc.GrpcHealthService._
import com.daml.platform.server.api.services.grpc.GrpcHealthServiceSpec._
import io.grpc.StatusRuntimeException
import io.grpc.health.v1.health.{HealthCheckRequest, HealthCheckResponse}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Span}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.DurationInt

final class GrpcHealthServiceSpec
    extends AsyncWordSpec
    with Matchers
    with Eventually
    with AkkaBeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Second)))

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "HealthService" should {
    "report SERVING if there are no health checks" in {
      val service = new GrpcHealthService(new HealthChecks, errorCodesVersionSwitcherMock)

      for {
        response <- service.check(HealthCheckRequest())
      } yield {
        response should be(servingResponse)
      }
    }

    "report SERVING if there is one healthy check" in {
      val service = new GrpcHealthService(
        new HealthChecks("component" -> healthyComponent),
        errorCodesVersionSwitcherMock,
      )

      for {
        response <- service.check(HealthCheckRequest())
      } yield {
        response should be(servingResponse)
      }
    }

    "report NOT_SERVING if there is one unhealthy check" in {
      val service = new GrpcHealthService(
        new HealthChecks("component" -> unhealthyComponent),
        errorCodesVersionSwitcherMock,
      )

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
        ),
        errorCodesVersionSwitcherMock,
      )

      service.check(HealthCheckRequest())
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
        ),
        errorCodesVersionSwitcherMock,
      )

      for {
        response <- service.check(HealthCheckRequest())
      } yield {
        response should be(notServingResponse)
      }
    }

    "report SERVING when querying a single, healthy component" in {
      val service = new GrpcHealthService(
        new HealthChecks("component" -> healthyComponent),
        errorCodesVersionSwitcherMock,
      )

      for {
        response <- service.check(HealthCheckRequest("component"))
      } yield {
        response should be(servingResponse)
      }
    }

    "report NOT_SERVING when querying a single, unhealthy component" in {
      val service = new GrpcHealthService(
        new HealthChecks("component" -> unhealthyComponent),
        errorCodesVersionSwitcherMock,
      )

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
        ),
        errorCodesVersionSwitcherMock,
      )

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
        ),
        errorCodesVersionSwitcherMock,
      )

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
          "component A" -> componentWithHealthBackedBy(() => componentAHealth),
          "component B" -> componentWithHealthBackedBy(() => componentBHealth),
          "component C" -> componentWithHealthBackedBy(() => componentCHealth),
        ),
        errorCodesVersionSwitcherMock,
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
          Vector(servingResponse, notServingResponse, servingResponse)
        )
      }

      componentAHealth = Unhealthy
      eventually {
        responseObserver.elements should be(
          Vector(servingResponse, notServingResponse, servingResponse, notServingResponse)
        )
      }

      // this won't emit a new response, because the overall health of the system didn't change.
      componentCHealth = Unhealthy
      eventually {
        responseObserver.elements should be(
          Vector(servingResponse, notServingResponse, servingResponse, notServingResponse)
        )
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
          )
        )
      }
      succeed
    }

    "observe changes in a single component's health" in {
      val responseObserver = new MockServerCallStreamObserver[HealthCheckResponse]

      var componentAHealth: HealthStatus = Healthy
      var componentBHealth: HealthStatus = Healthy
      var componentCHealth: HealthStatus = Healthy
      val service = new GrpcHealthService(
        new HealthChecks(
          "component A" -> componentWithHealthBackedBy(() => componentAHealth),
          "component B" -> componentWithHealthBackedBy(() => componentBHealth),
          "component C" -> componentWithHealthBackedBy(() => componentCHealth),
        ),
        errorCodesVersionSwitcherMock,
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
          Vector(servingResponse, notServingResponse, servingResponse)
        )
      }

      // this component won't affect the health of component C
      componentAHealth = Healthy
      eventually {
        responseObserver.elements should be(
          Vector(servingResponse, notServingResponse, servingResponse)
        )
      }
      succeed
    }
  }

  "fail gracefully when a non-existent component is checked (returns V1 error codes)" in {
    failOnNonExistentCheckedComponent(usesSelfServiceErrorCodes = false)
  }

  "fail gracefully when a non-existent component is checked (returns V2 error codes)" in {
    failOnNonExistentCheckedComponent(usesSelfServiceErrorCodes = true)
  }

  "fail gracefully when a non-existent component is watched (returns V1 error codes)" in {
    failOnNonExistentWatchedComponent(usesSelfServiceErrorCodes = false)
  }

  "fail gracefully when a non-existent component is watched (returns V2 error codes)" in {
    failOnNonExistentWatchedComponent(usesSelfServiceErrorCodes = true)
  }

  private def failOnNonExistentCheckedComponent(usesSelfServiceErrorCodes: Boolean) = {
    val service = new GrpcHealthService(
      new HealthChecks("component" -> unhealthyComponent),
      new ErrorCodesVersionSwitcher(usesSelfServiceErrorCodes),
    )

    for {
      throwable <- service.check(HealthCheckRequest("another component")).failed
    } yield assertErrorCode(usesSelfServiceErrorCodes, throwable)
  }

  private def failOnNonExistentWatchedComponent(usesSelfServiceErrorCodes: Boolean) = {
    val responseObserver = new MockServerCallStreamObserver[HealthCheckResponse]
    val service = new GrpcHealthService(
      new HealthChecks("component" -> unhealthyComponent),
      new ErrorCodesVersionSwitcher(usesSelfServiceErrorCodes),
    )

    service.watch(HealthCheckRequest("another component"), responseObserver)
    responseObserver.demandResponse()

    for {
      throwable <- responseObserver.completionFuture.failed
    } yield assertErrorCode(usesSelfServiceErrorCodes, throwable)
  }

  private def assertErrorCode(usesSelfServiceErrorCodes: Boolean, throwable: Throwable) =
    throwable match {
      case GrpcException.NOT_FOUND() if !usesSelfServiceErrorCodes => succeed
      case GrpcException.INVALID_ARGUMENT() if usesSelfServiceErrorCodes => succeed
      case ex => fail(s"Expected a NOT_FOUND error, but got $ex")
    }

  // On the happy flow, this parameter is not used.
  // Negative tests returning errors should explicitly instantiate it
  private def errorCodesVersionSwitcherMock: ErrorCodesVersionSwitcher =
    new ErrorCodesVersionSwitcher(false) {
      override def choose(
          v1: => StatusRuntimeException,
          v2: => StatusRuntimeException,
      ): StatusRuntimeException = {
        val _ = (v1, v2)
        fail("Should not be called")
      }
    }
}

object GrpcHealthServiceSpec {
  private val healthyComponent: ReportsHealth = () => Healthy

  private val unhealthyComponent: ReportsHealth = () => Unhealthy

  private def componentWithHealthBackedBy(fetchCurrentHealth: () => HealthStatus): ReportsHealth =
    () => fetchCurrentHealth()
}
