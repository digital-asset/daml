// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.lifecycle.{FlagCloseable, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.TemporaryStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class TemporaryStoreRegistry(
    nodeId: UniqueIdentifier,
    clock: Clock,
    crypto: Crypto,
    futureSupervisor: FutureSupervisor,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  private val registeredStores =
    TrieMap[TemporaryStore, TemporaryTopologyManager]()

  def createStore(storeId: TemporaryStore, protocolVersion: ProtocolVersion)(implicit
      traceContext: TraceContext
  ): Either[TopologyManagerError, TemporaryTopologyManager] = {
    logger.info(s"Creating temporary topology store $storeId")

    val loggerFactoryWithStore = loggerFactory.append("store", storeId.dbString.unwrap)

    lazy val store = new InMemoryTopologyStore(
      storeId,
      protocolVersion,
      loggerFactoryWithStore,
      timeouts,
    )

    lazy val manager = new TemporaryTopologyManager(
      nodeId,
      clock,
      crypto,
      store,
      timeouts,
      futureSupervisor,
      loggerFactoryWithStore,
    )
    registeredStores
      .putIfAbsent(storeId, manager) match {
      case Some(_) => Left(TopologyManagerError.TemporaryTopologyStoreAlreadyExists.Reject(storeId))
      case None => Right(manager)
    }
  }

  def dropStore(
      storeId: TemporaryStore
  )(implicit traceContext: TraceContext): Either[TopologyManagerError, Unit] = {
    logger.info(s"Dropping temporary topology store $storeId")
    registeredStores.remove(storeId) match {
      case Some(_) => Right(())
      case None => Left(TopologyManagerError.TopologyStoreUnknown.Failure(storeId))
    }
  }

  def managers(): Seq[TemporaryTopologyManager] = registeredStores.values.toList
  def stores(): Seq[TopologyStore[TemporaryStore]] = managers().map(_.store)

  override protected def onClosed(): Unit =
    LifeCycle.close(registeredStores.values.toList*)(logger)
}
