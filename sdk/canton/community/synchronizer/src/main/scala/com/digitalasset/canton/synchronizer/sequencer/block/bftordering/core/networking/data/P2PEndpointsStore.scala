// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.db.DbP2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.memory.InMemoryP2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait P2PEndpointsStore[E <: Env[E]] extends AutoCloseable {

  def listEndpoints(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Seq[P2PEndpoint]]
  protected final val listEndpointsActionName: String = "list BFT ordering P2P endpoints"

  def addEndpoint(endpoint: P2PEndpoint)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Boolean]
  protected final def addEndpointActionName(endpoint: P2PEndpoint): String =
    s"add BFT ordering P2P endpoint $endpoint"

  def removeEndpoint(endpointId: P2PEndpoint.Id)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Boolean]
  protected final def removeEndpointActionName(endpointId: P2PEndpoint.Id): String =
    s"remove BFT ordering P2P endpoint $endpointId"

  def clearAllEndpoints()(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Unit]
  protected final val clearAllEndpointsActionName: String = "clear all BFT ordering P2P endpoints"
}

object P2PEndpointsStore {

  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): P2PEndpointsStore[PekkoEnv] =
    storage match {
      case _: MemoryStorage =>
        new InMemoryP2PEndpointsStore()
      case dbStorage: DbStorage =>
        new DbP2PEndpointsStore(dbStorage, timeouts, loggerFactory)(executionContext)
    }
}
