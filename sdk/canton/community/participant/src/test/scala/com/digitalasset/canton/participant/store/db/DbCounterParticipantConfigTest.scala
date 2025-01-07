// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.participant.store.SlowCounterParticipantConfigTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait DbCounterParticipantConfigTest extends SlowCounterParticipantConfigTest { this: DbTest =>

  override def cleanDb(storage: DbStorage)(implicit traceContext: TraceContext): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table acs_slow_counter_participants",
        sqlu"truncate table acs_slow_participant_config",
        sqlu"truncate table acs_no_wait_counter_participants",
      ),
      functionFullName,
    )
  }

  "DbCounterParticipantConfigStore" should {
    behave like AcsCommitmentSlowCounterParticipantConfigStore((ec: ExecutionContext) =>
      new DbAcsCommitmentConfigStore(
        storage,
        timeouts,
        loggerFactory,
      )(ec)
    )
    behave like AcsCommitmentNoWaitParticipantConfigStore((ec: ExecutionContext) =>
      new DbAcsCommitmentConfigStore(
        storage,
        timeouts,
        loggerFactory,
      )(ec)
    )
  }
}

class DbCounterParticipantConfigTestH2 extends DbCounterParticipantConfigTest with H2Test
class DbCounterParticipantConfigTestPostgres
    extends DbCounterParticipantConfigTest
    with PostgresTest
