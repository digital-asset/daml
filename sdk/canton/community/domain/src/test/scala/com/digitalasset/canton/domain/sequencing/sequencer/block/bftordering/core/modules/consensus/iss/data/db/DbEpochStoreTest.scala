// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbEpochStoreTest extends AsyncWordSpec with BftSequencerBaseTest with EpochStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage)(implicit traceContext: TraceContext): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table ord_epochs",
        sqlu"truncate table ord_pbft_messages_in_progress",
        sqlu"truncate table ord_pbft_messages_completed",
      ),
      functionFullName,
    )
  }

  "DbEpochStore" should {
    behave like epochStore(() =>
      new DbEpochStore(storage, timeouts, loggerFactory)(executionContext)
    )
  }
}

class DbEpochStoreTestH2 extends DbEpochStoreTest with H2Test

class DbEpochStoreTestPostgres extends DbEpochStoreTest with PostgresTest
