// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import java.sql.Connection

import anorm.SqlParser.get
import anorm.SqlStringInterpolation
import com.daml.ledger.offset.Offset
import com.daml.platform.store.backend.common.{
  EventStorageBackendTemplate,
  ParameterStorageBackendTemplate,
}
import com.daml.platform.store.cache.LedgerEndCache

class H2EventStorageBackend(ledgerEndCache: LedgerEndCache)
    extends EventStorageBackendTemplate(
      queryStrategy = H2QueryStrategy,
      eventStrategy = H2EventStrategy,
      ledgerEndCache = ledgerEndCache,
      participantAllDivulgedContractsPrunedUpToInclusive =
        ParameterStorageBackendTemplate.participantAllDivulgedContractsPrunedUpToInclusive,
    ) {

  override def maxEventSequentialIdOfAnObservableEvent(
      offset: Offset
  )(connection: Connection): Option[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL"""
SELECT max_esi FROM (
  (SELECT max(event_sequential_id) AS max_esi FROM participant_events_consuming_exercise WHERE event_offset <= $offset GROUP BY event_offset ORDER BY event_offset DESC FETCH NEXT 1 ROW ONLY)
  UNION ALL
  (SELECT max(event_sequential_id) AS max_esi FROM participant_events_non_consuming_exercise WHERE event_offset <= $offset GROUP BY event_offset ORDER BY event_offset DESC FETCH NEXT 1 ROW ONLY)
  UNION ALL
  (SELECT max(event_sequential_id) AS max_esi FROM participant_events_create WHERE event_offset <= $offset GROUP BY event_offset ORDER BY event_offset DESC FETCH NEXT 1 ROW ONLY)
) AS t
ORDER BY max_esi DESC
FETCH NEXT 1 ROW ONLY;
       """.as(get[Long](1).singleOpt)(connection)
  }

  // Migration from mutable schema is not supported for H2
  override def isPruningOffsetValidAgainstMigration(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      connection: Connection,
  ): Boolean = true
}
