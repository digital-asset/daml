// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import cats.implicits.showInterpolator
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ShowUtil.showPretty
import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import io.grpc.protobuf.services.HealthStatusManager

import scala.concurrent.blocking
import scala.util.Try

/** This class updates gRPC health service with updates coming from Canton's ServiceHealth instances.
  * See https://github.com/grpc/grpc/blob/master/doc/health-checking.md
  * This class will update the health manager when new health state are being reported by the services, which will in turn
  * be available to external clients through the gRPC Health service.
  */
class GrpcHealthReporter(override val loggerFactory: NamedLoggerFactory)
    extends NamedLogging
    with NoTracing {

  private def allServicesAreServing(healthStatusManager: ServiceHealthStatusManager): Boolean = {
    healthStatusManager.services.map(_.getState).forall(_ == ServingStatus.SERVING)
  }

  /** Update a service in a health manager.
    * If the status is not SERVING, the aggregated health status will be updated to NOT_SERVING
    * If all statuses are SERVING, the aggregated health status will be updated to SERVING
    */
  private def updateHealthManager(
      healthStatusManager: ServiceHealthStatusManager,
      serviceHealth: HealthService,
  ): Unit = blocking {
    synchronized {
      val status = serviceHealth.getState
      logger.debug(
        show"${serviceHealth.name} in ${healthStatusManager.name} is ${status.name()}"
      )

      healthStatusManager.manager.setStatus(serviceHealth.name, status)

      // Update the default service status
      if (status != ServingStatus.SERVING) {
        logger.debug(
          s"${healthStatusManager.name} is ${ServingStatus.NOT_SERVING.name()}"
        )

        healthStatusManager.manager.setStatus(
          HealthStatusManager.SERVICE_NAME_ALL_SERVICES,
          ServingStatus.NOT_SERVING,
        )
      } else if (allServicesAreServing(healthStatusManager)) {
        logger.debug(
          s"${healthStatusManager.name} is ${ServingStatus.SERVING.name()}"
        )

        healthStatusManager.manager.setStatus(
          HealthStatusManager.SERVICE_NAME_ALL_SERVICES,
          ServingStatus.SERVING,
        )
        ()
      }
    }
  }

  /** Registers a gRPC health manager with a set of service identifiers.
    * These services will be available for health check in the health manager.
    * The "default" service aggregates all services health such that the default service is "SERVING"
    * if and only if all services are "SERVING"
    *
    * Should only be called once per health manager.
    */
  def registerHealthManager(
      healthStatusManager: ServiceHealthStatusManager
  ): Unit = {
    healthStatusManager.services.foreach(service =>
      service
        .registerOnHealthChange(new HealthListener {
          override def name: String = "GrpcHealthReporter"

          override def poke()(implicit traceContext: TraceContext): Unit = {
            Try(updateHealthManager(healthStatusManager, service))
              .recover {
                // Can happen if we update the status while a listening RPC is being cancelled
                case sre: io.grpc.StatusRuntimeException
                    if sre.getStatus.getCode.value() == io.grpc.Status.CANCELLED.getCode.value() =>
                  logger.info(
                    s"RPC was cancelled while updating health manager ${healthStatusManager.name} with service ${service.name}",
                    sre,
                  )
              }
              .discard[Try[Unit]]
          }
        })
        .discard[Boolean]
    )
  }
}
