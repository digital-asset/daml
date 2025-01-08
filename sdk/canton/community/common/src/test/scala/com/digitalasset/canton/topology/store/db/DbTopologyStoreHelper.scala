// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.config.CantonRequireTypes.String68
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbTest
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait DbTopologyStoreHelper {

  this: DbTest & TestEssentials =>

  // Using test name as discriminator to avoid db-unit-test clashes such as non-X DbTopologyStoreTest.
  // We reuse the `topology_dispatching` table from non-X topology management.
  private val discriminator = String68.tryCreate(getClass.getSimpleName.takeRight(40))
  override def cleanDb(storage: DbStorage)(implicit traceContext: TraceContext): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"delete from common_topology_transactions where store_id like $discriminator || '%'"
      ),
      operationName = s"${this.getClass}: Delete common_topology_transactions for $discriminator",
    )
  }

  protected val maxItemsInSqlQuery: PositiveInt = PositiveInt.tryCreate(20)

  protected def createTopologyStore(
      synchronizerId: SynchronizerId
  ): TopologyStore[SynchronizerStore] =
    new DbTopologyStore(
      storage,
      TopologyStoreId.SynchronizerStore(synchronizerId, discriminator.str),
      testedProtocolVersion,
      timeouts,
      loggerFactory,
      maxItemsInSqlQuery = maxItemsInSqlQuery,
    )
}
