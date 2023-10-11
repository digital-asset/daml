// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbTest
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}

import scala.concurrent.Future

trait DbTopologyStoreXHelper {

  this: DbTest & TestEssentials =>

  private val storeId = TopologyStoreId.DomainStore(
    DefaultTestIdentities.domainId,
    // Using test name as discriminator to avoid db-unit-test clashes such as non-X DbTopologyStoreTest.
    // We reuse the `topology_dispatching` table from non-X topology management.
    getClass.getSimpleName.takeRight(40),
  )
  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"delete from topology_transactions_x where store_id=${storeId.dbString}"
      ),
      operationName = s"${this.getClass}: Delete topology_transactions_x for ${storeId.dbString}",
    )
  }

  protected val maxItemsInSqlQuery: PositiveInt = PositiveInt.tryCreate(20)

  protected def createTopologyStore(): TopologyStoreX[DomainStore] =
    new DbTopologyStoreX(
      storage,
      storeId,
      maxDbConnections = PositiveInt.tryCreate(5),
      timeouts,
      loggerFactory,
      maxItemsInSqlQuery = maxItemsInSqlQuery,
    )
}
