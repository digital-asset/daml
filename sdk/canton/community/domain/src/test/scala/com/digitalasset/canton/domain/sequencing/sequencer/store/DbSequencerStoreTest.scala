// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait DbSequencerStoreTest extends SequencerStoreTest with MultiTenantedSequencerStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] =
    DbSequencerStoreTest.cleanSequencerTables(storage)

  "DbSequencerStore" should {
    behave like sequencerStore(() =>
      new DbSequencerStore(
        storage,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
        sequencerMember,
        blockSequencerMode = true,
      )
    )
    behave like multiTenantedSequencerStore(() =>
      new DbSequencerStore(
        storage,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
        sequencerMember,
        blockSequencerMode = true,
      )
    )
  }
}

object DbSequencerStoreTest {

  def cleanSequencerTables(
      storage: DbStorage
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[Unit] = {
    import storage.api.*

    storage.update(
      DBIO.seq(
        Seq(
          "members",
          "counter_checkpoints",
          "payloads",
          "watermarks",
          "events",
          "acknowledgements",
          "lower_bound",
        )
          .map(name => sqlu"truncate table sequencer_#$name")*
      ),
      functionFullName,
    )
  }
}

class SequencerStoreTestH2 extends DbSequencerStoreTest with H2Test

class SequencerStoreTestPostgres extends DbSequencerStoreTest with PostgresTest
