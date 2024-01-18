// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.admin.v0.MediatorAdministrationServiceGrpc
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, FlagCloseableAsync, SyncCloseable}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonMutableHandlerRegistry, GrpcDynamicService}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

trait MediatorReplicaManager extends NamedLogging with FlagCloseableAsync {

  protected val mediatorRuntimeFactoryRef
      : SingleUseCell[() => EitherT[Future, String, MediatorRuntime]] =
    new SingleUseCell

  protected def getMediatorRuntimeFactory()
      : Option[() => EitherT[Future, String, MediatorRuntime]] =
    mediatorRuntimeFactoryRef.get

  protected val mediatorRuntimeRef = new AtomicReference[Option[MediatorRuntime]](None)

  protected[canton] def mediatorRuntime: Option[MediatorRuntime] = mediatorRuntimeRef.get()

  protected val serviceUnavailableMessage = "Mediator replica is passive"

  val domainTimeService =
    new GrpcDynamicService(
      v0.DomainTimeServiceGrpc.SERVICE,
      serviceUnavailableMessage,
      loggerFactory,
    )

  def setup(
      adminServiceRegistry: CantonMutableHandlerRegistry,
      factory: () => EitherT[Future, String, MediatorRuntime],
      isActive: Boolean,
  )(implicit traceContext: TraceContext): Future[Unit]

  def isActive: Boolean

  def getTopologyQueueStatus: TopologyQueueStatus = TopologyQueueStatus(
    manager =
      mediatorRuntime.flatMap(_.mediator.topologyManagerStatusO).map(_.queueSize).getOrElse(0),
    dispatcher =
      mediatorRuntime.flatMap(_.mediator.domainOutboxStatusO).map(_.queueSize).getOrElse(0),
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

  val enterpriseAdminService =
    new GrpcDynamicService(
      MediatorAdministrationServiceGrpc.SERVICE,
      serviceUnavailableMessage,
      loggerFactory,
    )

  override def setup(
      adminServiceRegistry: CantonMutableHandlerRegistry,
      factory: () => EitherT[Future, String, MediatorRuntime],
      isActive: Boolean,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.debug("Setting up replica manager")

    mediatorRuntimeFactoryRef.putIfAbsent(factory).foreach { prev =>
      logger.warn(
        s"Mediator runtime factory was already initialized with $prev, ignoring new value"
      )
    }

    adminServiceRegistry
      .addServiceU(domainTimeService.serviceDescriptor)
    adminServiceRegistry
      .addServiceU(enterpriseAdminService.serviceDescriptor)

    val result = for {
      mediatorRuntime <- factory()
    } yield {
      mediatorRuntimeRef.set(Some(mediatorRuntime))
      domainTimeService.setInstance(mediatorRuntime.timeService)
      enterpriseAdminService.setInstance(mediatorRuntime.enterpriseAdministrationService)
    }

    EitherTUtil.toFuture(result.leftMap(new MediatorReplicaManagerException(_)))
  }

  override def isActive: Boolean = true
}

/** An unexpected error occurred while transitioning between replica states */
class MediatorReplicaManagerException(message: String) extends RuntimeException(message)
