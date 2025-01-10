// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30.MediatorAdministrationServiceGrpc
import com.digitalasset.canton.networking.grpc.{CantonMutableHandlerRegistry, GrpcDynamicService}
import com.digitalasset.canton.time.admin.v30
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

trait MediatorReplicaManager extends NamedLogging with FlagCloseableAsync {

  protected val mediatorRuntimeFactoryRef
      : SingleUseCell[() => EitherT[FutureUnlessShutdown, String, MediatorRuntime]] =
    new SingleUseCell

  protected def getMediatorRuntimeFactory
      : Option[() => EitherT[FutureUnlessShutdown, String, MediatorRuntime]] =
    mediatorRuntimeFactoryRef.get

  protected val mediatorRuntimeRef = new AtomicReference[Option[MediatorRuntime]](None)

  protected[canton] def mediatorRuntime: Option[MediatorRuntime] = mediatorRuntimeRef.get()

  protected val serviceUnavailableMessage = "Mediator replica is passive"

  val synchronizerTimeService =
    new GrpcDynamicService(
      v30.SynchronizerTimeServiceGrpc.SERVICE,
      serviceUnavailableMessage,
      loggerFactory,
    )

  def setup(
      adminServiceRegistry: CantonMutableHandlerRegistry,
      factory: () => EitherT[FutureUnlessShutdown, String, MediatorRuntime],
      isActive: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  def isActive: Boolean

  def getTopologyQueueStatus: TopologyQueueStatus = TopologyQueueStatus(
    manager = mediatorRuntime.map(_.mediator.topologyManagerStatus.queueSize).getOrElse(0),
    dispatcher = mediatorRuntime.map(_.mediator.synchronizerOutboxHandle.queueSize).getOrElse(0),
    clients = mediatorRuntime.map(x => x.mediator.topologyClient.numPendingChanges).getOrElse(0),
  )

  override def closeAsync(): Seq[AsyncOrSyncCloseable] = mediatorRuntimeRef
    .get()
    .toList
    .map(runtime => SyncCloseable("mediatorRuntime", runtime.close()))
}

/** Community version of the mediator replica manager.
  * Does not support high-availability.
  */
class CommunityMediatorReplicaManager(
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends MediatorReplicaManager {

  private val adminService =
    new GrpcDynamicService(
      MediatorAdministrationServiceGrpc.SERVICE,
      serviceUnavailableMessage,
      loggerFactory,
    )

  override def setup(
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

    val result = for {
      mediatorRuntime <- factory()
    } yield {
      mediatorRuntimeRef.set(Some(mediatorRuntime))
      synchronizerTimeService.setInstance(mediatorRuntime.timeService)
      adminService.setInstance(mediatorRuntime.administrationService)
    }

    EitherTUtil.toFutureUnlessShutdown(result.leftMap(new MediatorReplicaManagerException(_)))
  }

  override def isActive: Boolean = true
}

/** An unexpected error occurred while transitioning between replica states */
class MediatorReplicaManagerException(message: String) extends RuntimeException(message)
