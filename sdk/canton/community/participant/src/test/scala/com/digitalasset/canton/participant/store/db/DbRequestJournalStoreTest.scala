// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.BatchAggregatorConfig
import com.digitalasset.canton.participant.store.RequestJournalStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbRequestJournalStoreTest
    extends AsyncWordSpec
    with BaseTest
    with RequestJournalStoreTest
    with FailOnShutdown {
  this: DbTest =>

  val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("da::default"))

  override def cleanDb(storage: DbStorage)(implicit traceContext: TraceContext): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(sqlu"truncate table par_journal_requests"),
      functionFullName,
    )
  }

  "DbRequestJournalStore" should {
    behave like requestJournalStore(() =>
      new DbRequestJournalStore(
        IndexedDomain.tryCreate(domainId, 1),
        storage,
        BatchAggregatorConfig.defaultsForTesting,
        BatchAggregatorConfig.defaultsForTesting,
        timeouts,
        loggerFactory,
      )
    )
  }
}

class RequestJournalStoreTestH2 extends DbRequestJournalStoreTest with H2Test

class RequestJournalStoreTestPostgres extends DbRequestJournalStoreTest with PostgresTest
