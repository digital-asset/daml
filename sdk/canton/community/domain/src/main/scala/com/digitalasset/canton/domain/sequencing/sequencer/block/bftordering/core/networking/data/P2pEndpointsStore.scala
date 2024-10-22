// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking.data

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking.data.db.DbP2pEndpointsStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking.data.memory.InMemoryP2pEndpointsStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait P2pEndpointsStore[E <: Env[E]] {

  def listEndpoints(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Seq[Endpoint]]
  protected final val listEndpointsActionName: String = "list BFT ordering P2P endpoints"

  def addEndpoint(endpoint: Endpoint)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Boolean]
  protected final def addEndpointActionName(endpoint: Endpoint): String =
    s"add BFT ordering P2P endpoint $endpoint"

  def removeEndpoint(endpoint: Endpoint)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Boolean]
  protected final def removeEndpointActionName(endpoint: Endpoint): String =
    s"remove BFT ordering P2P endpoint $endpoint"

  def clearAllEndpoints()(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Unit]
  protected final val clearAllEndpointsActionName: String = "clear all BFT ordering P2P endpoints"
}

object P2pEndpointsStore {

  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): P2pEndpointsStore[PekkoEnv] =
    storage match {
      case _: MemoryStorage =>
        new InMemoryP2pEndpointsStore()
      case dbStorage: DbStorage =>
        new DbP2pEndpointsStore(dbStorage, timeouts, loggerFactory)(executionContext)
    }
}
