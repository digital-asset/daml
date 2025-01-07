// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.network.data.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.network.data.P2pEndpointsStoreTest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.networking.data.db.DbP2pEndpointsStore
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbP2pEndpointsStoreTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with P2pEndpointsStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage)(implicit traceContext: TraceContext): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table ord_p2p_endpoints"
      ),
      functionFullName,
    )
  }

  "DbP2pEndpointsStore" should {
    // This storage maintains information provided through configuration admin console and is not required
    //  nor meant to be idempotent; this excludes the idempotent testing wrapper normally present for DB tests.
    lazy val nonIdempotentStorage = storage.underlying
    behave like p2pEndpointsStore(() =>
      new DbP2pEndpointsStore(nonIdempotentStorage, timeouts, loggerFactory)(executionContext)
    )
  }
}

class DbP2pEndpointsStoreH2Test extends DbP2pEndpointsStoreTest with H2Test

class DbP2pEndpointsStorePostgresTest extends DbP2pEndpointsStoreTest with PostgresTest
