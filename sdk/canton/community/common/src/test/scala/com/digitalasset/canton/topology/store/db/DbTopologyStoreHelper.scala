// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbTest
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

trait DbTopologyStoreHelper {

  this: DbTest & TestEssentials =>

  @volatile
  private var storesToCleanup = List.empty[TopologyStore[_]]

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

  protected val maxItemsInSqlQuery: PositiveInt = PositiveInt.tryCreate(20)

  protected def mkStore(
      synchronizerId: SynchronizerId
  ): TopologyStore[TopologyStoreId.SynchronizerStore] = {
    val store = new DbTopologyStore(
      storage,
      TopologyStoreId.SynchronizerStore(synchronizerId),
      testedProtocolVersion,
      timeouts,
      loggerFactory,
      maxItemsInSqlQuery = maxItemsInSqlQuery,
    )
    storesToCleanup = store :: storesToCleanup
    store
  }
}
