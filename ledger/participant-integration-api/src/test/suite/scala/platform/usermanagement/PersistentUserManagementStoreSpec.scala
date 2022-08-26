// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.usermanagement

import com.codahale.metrics.MetricRegistry
import com.daml.api.util.TimeProvider
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.metrics.Metrics
import com.daml.platform.store.PersistentStoreSpecBase
import com.daml.platform.store.backend.StorageBackendProviderPostgres
import com.daml.platform.store.platform.usermanagement.UserManagementStoreTests
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.{Assertion, Ignore}

import scala.concurrent.Future

// TODO um-for-hub: Implement PersistentUserManagementStore changes
@Ignore
class PersistentUserManagementStoreSpec
    extends AsyncFreeSpec
    with UserManagementStoreTests
    with PersistentStoreSpecBase
    with StorageBackendProviderPostgres {

  override def testIt(f: UserManagementStore => Future[Assertion]): Future[Assertion] = {
    f(
      new PersistentUserManagementStore(
        dbSupport = dbSupport,
        metrics = new Metrics(new MetricRegistry()),
        timeProvider = TimeProvider.UTC,
        maxRightsPerUser = 100,
      )
    )
  }

}
