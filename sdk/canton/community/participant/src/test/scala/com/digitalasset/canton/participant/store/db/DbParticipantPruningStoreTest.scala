// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.{
  ParticipantPruningStore,
  ParticipantPruningStoreTest,
}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext

trait DbParticipantPruningStoreTest extends ParticipantPruningStoreTest { this: DbTest =>
  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update_(sqlu"delete from par_pruning_operation where name = $name", functionFullName)
  }

  override def mk(): ParticipantPruningStore =
    new DbParticipantPruningStore(name, storage, timeouts, loggerFactory)
}

class ParticipantPruningStoreTestH2 extends DbParticipantPruningStoreTest with H2Test
class ParticipantPruningStoreTestPostgres extends DbParticipantPruningStoreTest with PostgresTest
