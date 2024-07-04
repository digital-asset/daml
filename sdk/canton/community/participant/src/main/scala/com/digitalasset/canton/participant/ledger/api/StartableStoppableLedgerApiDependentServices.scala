// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import cats.Eval
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.admin.participant.v30.{PackageServiceGrpc, PingServiceGrpc}
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, CantonMutableHandlerRegistry}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.grpc.{GrpcPackageService, GrpcPingService}
import com.digitalasset.canton.participant.admin.{AdminWorkflowServices, PackageService}
import com.digitalasset.canton.participant.config.LocalParticipantConfig
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import io.grpc.ServerServiceDefinition
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem

import scala.concurrent.{ExecutionContextExecutor, blocking}

/** Holds and manages the lifecycle of all Canton services that use the Ledger API
  * and hence depend on the Ledger API server to be up.
  *
  * The services are started on participant initialization iff the participant
  * comes up as an active replica, otherwise they are started when the participant transitions to active.
  * On transition to passive participant state, these services are shutdown.
  *
  * It is also used to close and restart the services when the Ledger API server
  * needs to be taken down temporarily (e.g. for ledger pruning).
  */
class StartableStoppableLedgerApiDependentServices(
    config: LocalParticipantConfig,
    testingConfig: ParticipantNodeParameters,
    packageServiceE: Eval[PackageService],
    syncService: CantonSyncService,
    participantId: ParticipantId,
    clock: Clock,
    registry: CantonMutableHandlerRegistry,
    adminToken: CantonAdminToken,
    futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
    tracerProvider: TracerProvider,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    tracer: Tracer,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends AutoCloseable
    with NamedLogging {
  private type PackageServiceGrpc = ServerServiceDefinition
  private type PingServiceGrpc = ServerServiceDefinition
  private type ApiInfoServiceGrpc = ServerServiceDefinition

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var servicesRef =
    Option.empty[(AdminWorkflowServices, PackageServiceGrpc, PingServiceGrpc, ApiInfoServiceGrpc)]

  // Start on initialization if pertaining to an active participant replica.
  if (syncService.isActive()) start()(TraceContext.empty)

  def start()(implicit traceContext: TraceContext): Unit =
    blocking {
      synchronized {
        servicesRef match {
          case Some(_servicesStarted) =>
            logger.info(
              "Attempt to start Ledger API-dependent Canton services, but they are already started. Ignoring."
            )
          case None =>
            logger.debug("Starting Ledger API-dependent canton services")

            // Capture the packageService for this active session
            val packageService = packageServiceE.value
            val adminWorkflowServices =
              new AdminWorkflowServices(
                config,
                testingConfig,
                packageService,
                syncService,
                participantId.adminParty,
                adminToken,
                futureSupervisor,
                loggerFactory,
                clock,
                tracerProvider,
              )

            val (packageServiceGrpc, _) = registry.addService(
              PackageServiceGrpc
                .bindService(
                  new GrpcPackageService(
                    packageService,
                    syncService.synchronizeVettingOnConnectedDomains,
                    loggerFactory,
                  ),
                  ec,
                )
            )

            val (pingServiceGrpc, _) = registry
              .addService(
                PingServiceGrpc.bindService(
                  new GrpcPingService(adminWorkflowServices.ping, loggerFactory),
                  ec,
                )
              )

            val (apiInfoServiceGrpc, _) =
              registry
                .addService(
                  ApiInfoServiceGrpc.bindService(
                    new GrpcApiInfoService(CantonGrpcUtil.ApiName.AdminApi),
                    ec,
                  )
                )

            servicesRef =
              Some((adminWorkflowServices, packageServiceGrpc, pingServiceGrpc, apiInfoServiceGrpc))
        }
      }
    }

  override def close(): Unit =
    blocking {
      synchronized {
        servicesRef match {
          case Some(
                (adminWorkflowServices, packageServiceGrpc, pingGrpcService, apiInfiServiceGrpc)
              ) =>
            logger.debug("Stopping Ledger API-dependent Canton services")(TraceContext.empty)
            servicesRef = None
            registry.removeServiceU(pingGrpcService)
            registry.removeServiceU(packageServiceGrpc)
            registry.removeServiceU(apiInfiServiceGrpc)
            adminWorkflowServices.close()
          case None =>
            logger.debug("Ledger API-dependent Canton services already stopped")(TraceContext.empty)
        }
      }
    }
}
