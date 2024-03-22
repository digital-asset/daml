// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.participant.store.{
  ParticipantPruningStore,
  ParticipantPruningStoreTest,
}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}

import scala.concurrent.Future

trait DbParticipantPruningStoreTest extends ParticipantPruningStoreTest { this: DbTest =>
  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    storage.update_(sqlu"delete from pruning_operation where name = $name", functionFullName)
  }

  override def mk(): ParticipantPruningStore =
    new DbParticipantPruningStore(name, storage, timeouts, loggerFactory)
}

class ParticipantPruningStoreTestH2 extends DbParticipantPruningStoreTest with H2Test
class ParticipantPruningStoreTestPostgres extends DbParticipantPruningStoreTest with PostgresTest
