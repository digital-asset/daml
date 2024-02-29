// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.domain.sequencing.traffic.store.db.DbTrafficLimitsStore
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbTest
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbTrafficLimitsStoreTest extends AsyncWordSpec with BaseTest with TrafficLimitsStoreTest {
  this: DbTest =>
  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    if (testedProtocolVersion >= ProtocolVersion.v30)
      storage.update(DBIO.seq(sqlu"truncate table seq_top_up_events"), functionFullName)
    else Future.unit
  }

  "TrafficLimitsStore" should {
    behave like trafficLimitsStore(() =>
      new DbTrafficLimitsStore(
        storage,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )
    )
  }
}
