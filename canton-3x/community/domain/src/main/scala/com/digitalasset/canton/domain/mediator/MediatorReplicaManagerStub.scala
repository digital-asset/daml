// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.networking.grpc.CantonMutableHandlerRegistry
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait MediatorReplicaManagerStub extends AutoCloseable {

  def setup(
      adminServiceRegistry: CantonMutableHandlerRegistry,
      factory: () => EitherT[Future, String, MediatorRuntime],
      isActive: Boolean,
  )(implicit traceContext: TraceContext): Future[Unit]

  def isActive: Boolean

  def getTopologyQueueStatus(): TopologyQueueStatus

  protected[canton] def mediatorRuntime: Option[MediatorRuntime]
}
