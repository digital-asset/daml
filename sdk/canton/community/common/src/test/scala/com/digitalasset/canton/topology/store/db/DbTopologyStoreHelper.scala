// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.IndexedTopologyStoreId
import com.digitalasset.canton.store.db.DbTest
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

trait DbTopologyStoreHelper {

  this: DbTest & TestEssentials =>

  @volatile
  private var storesToCleanup = List.empty[TopologyStore[?]]

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    MonadUtil
      .sequentialTraverse_(storesToCleanup)(
        _.deleteAllData()
      )
      .map { _ =>
        storesToCleanup = Nil
      }

  protected val batchingConfig: BatchingConfig =
    BatchingConfig(
      maxItemsInBatch = PositiveInt.tryCreate(2),
      parallelism = PositiveInt.one,
      maxTopologyUpdateBatchSize = PositiveInt.tryCreate(2),
      maxTopologyWriteBatchSize = PositiveInt.tryCreate(2),
    )

  private lazy val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 10)

  protected def mkStore(
      synchronizerId: PhysicalSynchronizerId,
      testName: String,
  ): TopologyStore[TopologyStoreId.SynchronizerStore] = {
    val storeId = TopologyStoreId.SynchronizerStore(synchronizerId)
    // this one is in memory and will therefore complete immediately
    val storeIndex =
      Await.result(IndexedTopologyStoreId.indexed(indexedStringStore)(storeId), 10.seconds)
    val store = new DbTopologyStore(
      storage,
      storeId,
      storeIndex.onShutdown(throw new IllegalStateException("Shutdown")),
      testedProtocolVersion,
      timeouts,
      batchingConfig,
      loggerFactory.appendUnnamedKey("testName", testName),
    )
    storesToCleanup = store :: storesToCleanup
    store
  }
}
