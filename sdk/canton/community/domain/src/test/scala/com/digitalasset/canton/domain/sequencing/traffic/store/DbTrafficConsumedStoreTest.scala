// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.domain.sequencing.traffic.store.db.DbTrafficConsumedStore
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbTest
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbTrafficConsumedStoreTest extends AsyncWordSpec with BaseTest with TrafficConsumedStoreTest {
  this: DbTest =>
  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(sqlu"truncate table seq_traffic_control_consumed_journal"),
      functionFullName,
    )
  }

  "TrafficConsumedStore" should {
    behave like trafficConsumedStore(() =>
      new DbTrafficConsumedStore(
        storage,
        timeouts,
        loggerFactory,
      )
    )
  }
}
