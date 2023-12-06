// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.{IndexedDomain, SequencedEventStoreTest}
import com.digitalasset.canton.topology.DomainId
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbSequencedEventStoreTest extends AsyncWordSpec with BaseTest with SequencedEventStoreTest {
  this: DbTest =>

  def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*

    storage.update(
      DBIO.seq(
        sqlu"truncate table sequenced_events",
        sqlu"truncate table sequenced_event_store_pruning",
      ),
      operationName = s"${this.getClass}: truncate table sequenced_events tables",
    )
  }

  "DbSequencedEventStore" should {
    behave like sequencedEventStore(ec =>
      new DbSequencedEventStore(
        storage,
        SequencerClientDiscriminator.fromIndexedDomainId(
          IndexedDomain.tryCreate(DomainId.tryFromString("da::default"), 1)
        ),
        testedProtocolVersion,
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      )(ec)
    )
  }
}

class SequencedEventStoreTestH2 extends DbSequencedEventStoreTest with H2Test

class SequencedEventStoreTestPostgres extends DbSequencedEventStoreTest with PostgresTest
