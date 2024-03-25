// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.SingleDimensionEventLogTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbSingleDimensionEventLogTest
    extends AsyncWordSpec
    with BaseTest
    with SingleDimensionEventLogTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    storage.update_(
      DBIO.seq(
        sqlu"delete from par_event_log where log_id = $id" // table shared with other tests
      ),
      operationName = s"${this.getClass}: clean db",
    )
  }

  "DbSingleDimensionEventLog" should {
    behave like singleDimensionEventLog(() =>
      new DbSingleDimensionEventLog(
        id,
        storage,
        InMemoryIndexedStringStore(),
        testedReleaseProtocolVersion,
        timeouts,
        loggerFactory,
      )
    )
  }
}

class SingleDimensionEventLogTestH2 extends DbSingleDimensionEventLogTest with H2Test

class SingleDimensionEventLogTestPostgres extends DbSingleDimensionEventLogTest with PostgresTest
