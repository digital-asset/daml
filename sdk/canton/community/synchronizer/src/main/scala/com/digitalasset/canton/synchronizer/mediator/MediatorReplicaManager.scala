// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.EitherT
import cats.syntax.option.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30.{
  MediatorAdministrationServiceGrpc,
  MediatorInspectionServiceGrpc,
}
import com.digitalasset.canton.networking.grpc.{CantonMutableHandlerRegistry, GrpcDynamicService}
import com.digitalasset.canton.replica.{ReplicaManager, ReplicaState}
import com.digitalasset.canton.time.admin.v30
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

/** Manages replicas of a single Mediator instance. Passive instances are currently entirely passive
  * and have no components running at runtime. When becoming active a [[MediatorRuntime]] is started
  * and connected to the admin services. When becoming passive the running [[MediatorRuntime]] is
  * shutdown and the admin services are disconnected.
  *
  * If the admin services are called while passive every method will return an unavailable response.
  */
class MediatorReplicaManager(
    exitOnFatalFailures: Boolean,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit
    ec: ExecutionContext
) extends ReplicaManager(exitOnFatalFailures, timeouts, loggerFactory, futureSupervisor)
    with NamedLogging
    with FlagCloseableAsync {

  private val mediatorRuntimeFactoryRef
      : SingleUseCell[() => EitherT[FutureUnlessShutdown, String, MediatorRuntime]] =
    new SingleUseCell

  private def getMediatorRuntimeFactory
      : Option[() => EitherT[FutureUnlessShutdown, String, MediatorRuntime]] =
    mediatorRuntimeFactoryRef.get

  private val mediatorRuntimeRef = new AtomicReference[Option[MediatorRuntime]](None)

  protected[canton] def mediatorRuntime: Option[MediatorRuntime] = mediatorRuntimeRef.get()

  private val serviceUnavailableMessage = "Mediator replica is passive"

  private val synchronizerTimeService =
    new GrpcDynamicService(
      v30.SynchronizerTimeServiceGrpc.SERVICE,
      serviceUnavailableMessage,
      loggerFactory,
    )

  private val scanService =
    new GrpcDynamicService(
      MediatorInspectionServiceGrpc.SERVICE,
      serviceUnavailableMessage,
      loggerFactory,
    )

  private val adminService =
    new GrpcDynamicService(
      MediatorAdministrationServiceGrpc.SERVICE,
      serviceUnavailableMessage,
      loggerFactory,
    )

  def setup(
      adminServiceRegistry: CantonMutableHandlerRegistry,
      factory: () => EitherT[FutureUnlessShutdown, String, MediatorRuntime],
      isActive: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    logger.debug("Setting up replica manager")

    mediatorRuntimeFactoryRef.putIfAbsent(factory).foreach { prev =>
      logger.warn(
        s"Mediator runtime factory was already initialized with $prev, ignoring new value"
      )
    }

    adminServiceRegistry
      .addServiceU(synchronizerTimeService.serviceDescriptor)
    adminServiceRegistry
      .addServiceU(adminService.serviceDescriptor)
    adminServiceRegistry.addServiceU(scanService.serviceDescriptor)
    if (isActive) {
      // we intentionally transition to active here to have it bring up the mediator runtime for the active instance
      setActive()
    } else {
      // for passive instances we don't want the transition to occur so just set its initialization state
      setInitialState(ReplicaState.Passive)
      FutureUnlessShutdown.unit
    }
  }

  private def isInitialized: Boolean = getMediatorRuntimeFactory.isDefined

  /*
   * Override the default setActive behavior to only actually transition to active after the mediator replica manager
   * has been properly initialized.
   * Without this precaution, the mediator could become active during one of the startup stages before it is initialized.
   * This causes the mediator to not actually perform the transaction actions (initializing the mediator runtime, etc...)
   * and it will not properly complete the startup.
   */
  override def setActive(): FutureUnlessShutdown[Unit] =
    if (isInitialized) super.setActive()
    else {
      TraceContext.withNewTraceContext("active") { implicit traceContext =>
        logger.info("Not transitioning to active, because the mediator is not yet initialized.")
      }
      FutureUnlessShutdown.unit
    }

  /*
   * Override the default setPassive behavior to only actually transition to passive after the mediator replica manager
   * has been properly initialized.
   */
  override def setPassive(): FutureUnlessShutdown[Unit] =
    if (isInitialized) super.setPassive()
    else {
      TraceContext.withNewTraceContext("passive") { implicit traceContext =>
        logger.info("Not transitioning to passive, because the mediator is not yet initialized.")
      }
      FutureUnlessShutdown.unit
    }

  override protected def transitionToActive()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    getMediatorRuntimeFactory match {
      case Some(factory) =>
        EitherTUtil
          .toFutureUnlessShutdown(
            factory
              .apply()
              .leftMap(new MediatorReplicaManagerException(_))
          )
          .map { mediatorRuntime =>
            mediatorRuntimeRef.set(mediatorRuntime.some)
            synchronizerTimeService.setInstance(mediatorRuntime.timeService)
            adminService.setInstance(mediatorRuntime.administrationService)
            scanService.setInstance(mediatorRuntime.inspectionService)
          }
      case None =>
        logger.debug(
          "Mediator runtime factory is not set." +
            " This means the mediator has not been initialized yet."
        )
        FutureUnlessShutdown.unit
    }

  override protected def transitionToPassive()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure {
      synchronizerTimeService.clear()
      adminService.clear()
      scanService.clear()
      mediatorRuntime foreach { mediatorRuntime =>
        logger.debug("Closing Mediator Runtime")
        mediatorRuntime.close()
      }
      mediatorRuntimeRef.set(None)
    }

  def getTopologyQueueStatus: TopologyQueueStatus = TopologyQueueStatus(
    manager = mediatorRuntime.map(_.mediator.topologyManagerStatus.queueSize).getOrElse(0),
    dispatcher = mediatorRuntime.map(_.mediator.synchronizerOutboxHandle.queueSize).getOrElse(0),
    clients = mediatorRuntime.map(x => x.mediator.topologyClient.numPendingChanges).getOrElse(0),
  )

  override def closeAsync(): Seq[AsyncOrSyncCloseable] = super.closeAsync() ++ mediatorRuntimeRef
    .get()
    .toList
    .map(runtime => SyncCloseable("mediatorRuntime", runtime.close()))
}

/** An unexpected error occurred while transitioning between replica states */
class MediatorReplicaManagerException(message: String) extends RuntimeException(message)
