// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import org.apache.pekko.actor.ActorSystem
import com.digitalasset.canton.config.{CheckConfig, ProcessingTimeout}
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.HealthMetrics
import com.digitalasset.canton.participant.ParticipantNode
import com.digitalasset.canton.participant.admin.PingService
import com.digitalasset.canton.time.{Clock, WallClock}
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherUtil

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Check to determine a health check response */
trait HealthCheck extends AutoCloseable {

  /** Ask the check to decide whether we're healthy.
    * The future should complete successfully with a HealthCheckResult.
    * If the future fails this implies there was an error performing the check itself.
    */
  def isHealthy(implicit traceContext: TraceContext): Future[HealthCheckResult]
  override def close(): Unit = ()
}

/** Constant response for a health check (used by the always-healthy configuration) */
final case class StaticHealthCheck(private val result: HealthCheckResult) extends HealthCheck {
  override def isHealthy(implicit traceContext: TraceContext): Future[HealthCheckResult] =
    Future.successful(result)
}

/** Pings the supplied participant to determine health.
  * Will be considered unhealthy if unable to resolve the ping service for the participant alias (likely due to startup and initialization).
  * Ping success considered healthy, ping failure considered unhealthy.
  * If the ping future fails (rather than completing successfully with a failure), it will be converted to a unhealthy response and the exception will be logged at DEBUG level.
  */
class PingHealthCheck(
    environment: Environment,
    participantAlias: String,
    timeout: FiniteDuration,
    metrics: HealthMetrics,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends HealthCheck
    with NamedLogging {

  private val pingLatencyTimer = metrics.pingLatency

  override def isHealthy(implicit traceContext: TraceContext): Future[HealthCheckResult] =
    getParticipant match {
      case Left(failed) => Future.successful(failed)
      case Right(participant) =>
        val partyId = participant.id.uid
        val pingService = participant.ledgerApiDependentCantonServices.adminWorkflowServices.ping
        ping(pingService, partyId)
    }

  private def getParticipant: Either[HealthCheckResult, ParticipantNode] =
    for {
      init <- Option(
        environment.participants
      ) // if this check is called before the collection has been initialized it will be null, so be very defensive
        .flatMap(_.getRunning(participantAlias))
        .toRight(Unhealthy("participant is not started"))
      participant <- init.getNode.toRight(Unhealthy("participant has not been initialized"))
      _ <- Either.cond(
        participant.readyDomains.exists(_._2),
        (),
        Unhealthy("participant is not connected to any domains"),
      )
    } yield participant

  private def ping(pingService: PingService, partyId: UniqueIdentifier)(implicit
      traceContext: TraceContext
  ): Future[HealthCheckResult] = {
    val timer = pingLatencyTimer.time()
    val started = Instant.now()
    val pingResult = for {
      result <- pingService.ping(Set(partyId.toProtoPrimitive), Set(), timeout.toMillis)
    } yield {
      timer.stop()
      result match {
        case PingService.Success(roundTripTime, _) =>
          logger.debug(s"Health check ping completed in ${roundTripTime}")
          Healthy
        case PingService.Failure =>
          val elapsedTime = Duration.between(started, Instant.now)
          logger.warn(s"Health check ping failed (elapsed time ${elapsedTime.toMillis}ms)")
          Unhealthy("ping failure")
      }
    }

    pingResult recover { case NonFatal(ex) =>
      logger.warn("health check ping failed", ex)
      Unhealthy("ping failed")
    }
  }
}

/** For components that simply flag whether they are active or not, just return that.
  * @param isActive should return a Right if the instance is active,
  *                 should return Left with a message to be returned on the health endpoint if not.
  */
class IsActiveCheck(
    isActive: () => Either[String, Unit],
    protected val loggerFactory: NamedLoggerFactory,
) extends HealthCheck {
  override def isHealthy(implicit traceContext: TraceContext): Future[HealthCheckResult] =
    Future.successful {
      isActive().fold(Unhealthy, _ => Healthy)
    }
}

/** Rather than executing a check for every isHealthy call periodically run the check and cache the result, and return this cached value for isHealthy.
  */
class PeriodicCheck(
    clock: Clock,
    interval: FiniteDuration,
    protected val loggerFactory: NamedLoggerFactory,
)(check: HealthCheck)(implicit executionContext: ExecutionContext)
    extends HealthCheck
    with NamedLogging {

  /** Once closed we should prevent checks from being run and scheduled */
  private val closed = new AtomicBoolean(false)

  /** Cached value that will hold the previous health check result.
    * It is initialized by calling setupNextCheck so until the first result completes it will hold a pending future.
    */
  private val lastCheck = new AtomicReference[Future[HealthCheckResult]](setupFirstCheck)

  /** Returns the health check promise rather than updating lastCheck, so is suitable for initializing lastCheck */
  private def setupFirstCheck: Future[HealthCheckResult] = {
    val isHealthy = TraceContext.withNewTraceContext(check.isHealthy(_))
    isHealthy onComplete { _ =>
      setupNextCheck()
    }
    isHealthy
  }

  /** Runs the check and when completed updates the value of lastCheck */
  private def runCheck(): Unit = if (!closed.get()) {
    val isHealthy = TraceContext.withNewTraceContext(check.isHealthy(_))

    isHealthy onComplete { _ =>
      lastCheck.set(isHealthy)
      setupNextCheck()
    }
  }

  private def setupNextCheck(): Unit =
    if (!closed.get()) {
      val _ = clock.scheduleAfter(_ => runCheck(), Duration.ofMillis(interval.toMillis))
    }

  override def close(): Unit = {
    closed.set(true)
    Lifecycle.close(clock)(logger)
  }

  override def isHealthy(implicit traceContext: TraceContext): Future[HealthCheckResult] =
    lastCheck.get()
}

object HealthCheck {
  def apply(
      config: CheckConfig,
      metrics: HealthMetrics,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(environment: Environment)(implicit system: ActorSystem): HealthCheck = {
    implicit val executionContext = system.dispatcher
    config match {
      case CheckConfig.AlwaysHealthy =>
        StaticHealthCheck(Healthy)
      case CheckConfig.Ping(participantAlias, interval, timeout) =>
        // only ping periodically rather than on every health check
        new PeriodicCheck(
          new WallClock(timeouts, loggerFactory.appendUnnamedKey("clock", "ping-health-check")),
          interval.underlying,
          loggerFactory,
        )(
          new PingHealthCheck(
            environment,
            participantAlias,
            timeout.underlying,
            metrics,
            loggerFactory,
          )
        )

      case CheckConfig.IsActive(participantO) =>
        val configuredParticipants = environment.config.participantsByString

        val participantName = participantO match {
          case Some(configName) =>
            if (configuredParticipants.contains(configName)) configName
            else sys.error(s"Participant with name '$configName' is not configured")
          case None =>
            configuredParticipants.headOption
              .map(_._1)
              .getOrElse(
                s"IsActive health check must be configured with the participant name to check as there are many participants configured for this environment"
              )
        }

        def isActive: Either[String, Unit] =
          for {
            participant <- environment.participants
              .getRunning(participantName)
              .toRight("Participant is not running")
            runningParticipant <- participant.getNode.toRight("Participant is not initialized")
            _ <- EitherUtil.condUnitE(
              runningParticipant.sync.isActive(),
              "Participant is not active",
            )
          } yield ()

        new IsActiveCheck(() => isActive, loggerFactory)
    }
  }
}
