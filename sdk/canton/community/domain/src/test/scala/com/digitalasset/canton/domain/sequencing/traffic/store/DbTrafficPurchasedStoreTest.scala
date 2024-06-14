// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.BatchAggregatorConfig
import com.digitalasset.canton.domain.sequencing.traffic.store.db.DbTrafficPurchasedStore
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbTest
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbTrafficPurchasedStoreTest
    extends AsyncWordSpec
    with BaseTest
    with TrafficPurchasedStoreTest {
  this: DbTest =>
  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(sqlu"truncate table seq_traffic_control_balance_updates"),
      functionFullName,
    )
  }

  "TrafficPurchasedStore" should {
    behave like trafficPurchasedStore(() =>
      new DbTrafficPurchasedStore(
        BatchAggregatorConfig.NoBatching,
        storage,
        timeouts,
        loggerFactory,
      )
    )
  }
}
