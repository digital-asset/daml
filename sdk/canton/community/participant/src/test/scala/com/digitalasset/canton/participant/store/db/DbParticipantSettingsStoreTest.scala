// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.ParticipantSettingsStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext

trait DbParticipantSettingsStoreTest extends ParticipantSettingsStoreTest with DbTest {

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update_(sqlu"truncate table par_settings", functionFullName)
  }

  def mk(s: DbStorage): DbParticipantSettingsStore =
    new DbParticipantSettingsStore(
      s,
      timeouts,
      futureSupervisor,
      exitOnFatalFailures = true,
      loggerFactory,
    )(
      parallelExecutionContext
    )

  "DbParticipantResourceManagementStore" must {
    behave like participantSettingsStore(() => mk(storage))
  }

}

class ParticipantSettingsStoreTestH2 extends DbParticipantSettingsStoreTest with H2Test

class ParticipantSettingsStoreTestPostgres extends DbParticipantSettingsStoreTest with PostgresTest
