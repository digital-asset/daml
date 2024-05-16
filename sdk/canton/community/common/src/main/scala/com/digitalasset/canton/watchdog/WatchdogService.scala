// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.watchdog

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.{
  NonNegativeFiniteDuration,
  PositiveFiniteDuration,
  ProcessingTimeout,
}
import com.digitalasset.canton.health.LivenessHealthService
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.health.v1.HealthCheckResponse.ServingStatus

import java.util.concurrent.{ScheduledFuture, TimeUnit}

/** A watchdog service with it's own executor that checks if a node or service is alive and kills it if it is not.
  * @param checkInterval Periodicity of the check
  * @param checkIsAlive Function that checks if the service is alive, truthy means alive
  * @param killAction Function to execute if the service is not alive
  */
class WatchdogService(
    checkInterval: PositiveFiniteDuration,
    checkIsAlive: => Boolean,
    killDelay: NonNegativeFiniteDuration,
    killAction: => Unit,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends FlagCloseable
    with NamedLogging {
  private val scheduler =
    Threading.singleThreadScheduledExecutor("WatchdogService", noTracingLogger)

  private val checkAndKillTask = new Runnable {
    override def run(): Unit = {
      performUnlessClosing("WatchdogService-checkAndKillTask") {
        if (!checkIsAlive) {
          logger.underlying.error(
            s"Watchdog detected that the service is not alive. Scheduling to kill the service after a delay of $killDelay."
          )
          checkAndKillFuture.cancel(false)
          scheduler.schedule(
            new Runnable {
              override def run(): Unit = {
                logger.underlying.error("Watchdog is killing the service now.")
                killAction
              }
            },
            killDelay.underlying.toMicros,
            TimeUnit.MICROSECONDS,
          ): Unit
        } else {
          logger.underlying.trace("Watchdog detected that the service is alive.")
        }
      }(TraceContext.empty).onShutdown {
        logger.underlying.info("Watchdog service is shutting down.")
      }
    }
  }

  private val checkAndKillFuture: ScheduledFuture[?] = scheduler.scheduleAtFixedRate(
    checkAndKillTask,
    checkInterval.underlying.toMicros,
    checkInterval.underlying.toMicros,
    TimeUnit.MICROSECONDS,
  )
  logger.underlying.info(s"Watchdog service started with check interval $checkInterval")

  override def onClosed(): Unit = {

    scheduler.shutdownNow(): Unit
  }
}

object WatchdogService {

  /** Create a watchdog service that `sys.exit` when `healthService` returns `NOT_SERVING` status.
    * @param checkInterval Periodicity of the check
    * @param healthService Health service to check the status of
    * @return
    */
  def SysExitOnNotServing(
      checkInterval: PositiveFiniteDuration,
      killDelay: NonNegativeFiniteDuration,
      healthService: LivenessHealthService,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  ): WatchdogService =
    new WatchdogService(
      checkInterval,
      healthService.getState == ServingStatus.SERVING,
      killDelay,
      sys.exit(37),
      loggerFactory,
      timeouts,
    )
}
