// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonCommunityConfig
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.environment.{Environment, ParticipantNodes}
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.WorkflowId
import com.digitalasset.canton.metrics.HealthMetrics
import com.digitalasset.canton.participant.admin.{AdminWorkflowServices, PingService}
import com.digitalasset.canton.participant.ledger.api.StartableStoppableLedgerApiDependentServices
import com.digitalasset.canton.participant.{ParticipantNode, ParticipantNodeBootstrap}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}

/** Stub health check that returns the configured response to all isHealthy calls.
  * Counts how many times isHealthy has been called.
  * Not thread safe.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class StubbedCheck(
    initialValue: Future[HealthCheckResult] =
      Future.failed(new RuntimeException("check result not stubbed"))
) extends HealthCheck {
  var callCount = 0
  var nextResult: Future[HealthCheckResult] = initialValue

  override def isHealthy(implicit traceContext: TraceContext): Future[HealthCheckResult] = {
    callCount += 1
    nextResult
  }
}

class HealthCheckTest extends AsyncWordSpec with BaseTest {

  "ping check" should {
    val participant = "the_participant_alias"

    "return unhealthy if running participants are not yet available" in
      loggerFactory.suppressWarningsAndErrors {
        healthCheck(withNullParticipants()).isHealthy.map {
          inside(_) { case Unhealthy(message) =>
            message should include("not started")
          }
        }
      }

    "return unhealthy if running participant is not running" in
      loggerFactory.suppressWarningsAndErrors {
        healthCheck(withNotStartedParticipant()).isHealthy.map {
          inside(_) { case Unhealthy(message) =>
            message should include("not started")
          }
        }
      }

    "return unhealthy if running participant is not initialized" in
      loggerFactory.suppressWarningsAndErrors {
        healthCheck(withNotInitializedParticipant()).isHealthy.map {
          inside(_) { case Unhealthy(message) =>
            message should include("not been initialized")
          }
        }
      }

    "return unhealthy if initialized participant is still not connected" in
      loggerFactory.suppressWarningsAndErrors {
        healthCheck(withNotConnectedParticipant()).isHealthy.map {
          inside(_) { case Unhealthy(message) =>
            message should include("not connected to any domains")
          }
        }
      }

    "return unhealthy if ping fails with an unexpected error" in
      loggerFactory.suppressWarningsAndErrors {
        healthCheck(withPingResult(Future.failed(new RuntimeException("Unexpected")))).isHealthy
          .map {
            inside(_) { case Unhealthy(message) =>
              message shouldBe "ping failed"
            }
          }
      }

    "return unhealthy if ping fails" in
      loggerFactory.suppressWarningsAndErrors {
        healthCheck(withPingResult(Future.successful(PingService.Failure))).isHealthy.map {
          inside(_) { case Unhealthy(message) =>
            message shouldBe "ping failure"
          }
        }
      }

    "return healthy if ping succeeds" in
      healthCheck(
        withPingResult(Future.successful(PingService.Success(Duration.ofSeconds(1), participant)))
      ).isHealthy
        .map {
          _ should matchPattern { case Healthy =>
          }
        }

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    def withNullParticipants(): Environment = mockEnvironment { env =>
      when(env.environment.participants).thenReturn(null)
    }

    def withNotStartedParticipant(): Environment = mockEnvironment { mocks =>
      when(mocks.participants.getRunning(participant)).thenReturn(None)
    }

    def withNotInitializedParticipant(): Environment = mockEnvironment { mocks =>
      val init = mock[ParticipantNodeBootstrap]
      when(init.name).thenReturn(InstanceName.tryCreate(participant))
      when(init.getNode).thenReturn(None)
      when(mocks.participants.getRunning(participant)).thenReturn(Some(init))
    }

    def withNotConnectedParticipant(): Environment = mockEnvironment { mocks =>
      val init = mock[ParticipantNodeBootstrap]
      val node = mock[ParticipantNode]
      when(node.readyDomains).thenReturn(Map[DomainId, Boolean]())
      when(init.getNode).thenReturn(Some(node))
      when(mocks.participants.getRunning(participant)).thenReturn(Some(init))
    }

    def withPingResult(result: Future[PingService.Result]): Environment = mockEnvironment { mocks =>
      val init = mock[ParticipantNodeBootstrap]
      val node = mock[ParticipantNode]
      val id = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive(s"${participant}::test"))
      val ledgerApiDependentServices = mock[StartableStoppableLedgerApiDependentServices]
      val adminWorkflowServices = mock[AdminWorkflowServices]
      val pingService = mock[PingService]

      when(init.getNode).thenReturn(Some(node))
      when(node.id).thenReturn(id)
      when(node.readyDomains).thenReturn(Map(DomainId.tryFromString("test::test") -> true))
      when(node.ledgerApiDependentCantonServices).thenReturn(ledgerApiDependentServices)
      when(ledgerApiDependentServices.adminWorkflowServices(any[TraceContext]))
        .thenReturn(adminWorkflowServices)
      when(adminWorkflowServices.ping).thenReturn(pingService)
      when(
        pingService
          .ping(
            any[Set[String]],
            any[Set[String]],
            anyLong,
            anyLong,
            anyLong,
            any[Option[WorkflowId]],
            any[String],
          )(any[TraceContext])
      )
        .thenReturn(result)

      when(mocks.participants.getRunning(participant)).thenReturn(Some(init))
    }

    trait MockEnvironment extends Environment {
      override type Config = CantonCommunityConfig
    }

    trait MockEnvironmentInstance {
      val environment = mock[MockEnvironment]
      val participants = mock[ParticipantNodes[
        ParticipantNodeBootstrap,
        ParticipantNode,
        MockEnvironment#Config#ParticipantConfigType,
      ]]

      when(environment.participants).thenReturn(participants)
    }

    def mockEnvironment(setup: MockEnvironmentInstance => Unit): Environment = {
      val mockEnvironment = new MockEnvironmentInstance {}
      setup(mockEnvironment)
      mockEnvironment.environment
    }

    def healthCheck(environment: Environment) =
      new PingHealthCheck(
        environment,
        participant,
        10.seconds,
        new HealthMetrics(MetricName("test"), new MetricRegistry()),
        loggerFactory,
      )
  }

  "periodic check" should {
    "only runs check on creation and intervals" in {
      val interval = 1.second
      val underlyingCheck = new StubbedCheck
      val simClock = new SimClock(loggerFactory = loggerFactory)
      val firstPromise = Promise[HealthCheckResult]()
      underlyingCheck.nextResult = firstPromise.future
      val periodicCheck = new PeriodicCheck(simClock, interval, loggerFactory)(underlyingCheck)

      // should have been called on creation
      underlyingCheck.callCount shouldBe 1

      val firstResultFuture = periodicCheck.isHealthy
      // should have just returned the original future
      underlyingCheck.callCount shouldBe 1

      // complete the first underlyingCheck
      firstPromise.trySuccess(Healthy)

      for {
        firstResult <- firstResultFuture
        _ = firstResult shouldBe Healthy
        // calling again should just return the same future result and immediately resolve
        _ <- periodicCheck.isHealthy
        // and call count should remain the same
        _ = underlyingCheck.callCount shouldBe 1
        // setup next underlyingCheck result
        secondPromise = Promise[HealthCheckResult]()
        _ = underlyingCheck.nextResult = secondPromise.future
        // when time passes beyond the interval it should trigger another underlyingCheck
        _ = simClock.advance(Duration.ofMillis(interval.toMillis + 1))
        _ = underlyingCheck.callCount shouldBe 2
        // however the periodic underlyingCheck is still returning the prior result until the next underlyingCheck has completed
        anotherResult <- periodicCheck.isHealthy
        _ = anotherResult shouldBe Healthy
        // now lets complete the next call
        _ = secondPromise.trySuccess(Unhealthy(":("))
        // now the period underlyingCheck should return this unhealthy
        finalResult <- periodicCheck.isHealthy
      } yield finalResult shouldBe Unhealthy(":(")
    }

    "not perform checks after being closed" in {
      val interval = 1.second
      val underlyingCheck = new StubbedCheck(Future.successful(Healthy))
      val simClock = new SimClock(loggerFactory = loggerFactory)
      val periodicCheck = new PeriodicCheck(simClock, interval, loggerFactory)(underlyingCheck)

      for {
        _ <- periodicCheck.isHealthy
        initialCallCount = underlyingCheck.callCount
        _ = periodicCheck.close()
        _ = simClock.advance(Duration.ofMillis(interval.toMillis + 1))
        _ <- periodicCheck.isHealthy
        nextCallCount = underlyingCheck.callCount
      } yield nextCallCount should equal(initialCallCount)
    }
  }
}
