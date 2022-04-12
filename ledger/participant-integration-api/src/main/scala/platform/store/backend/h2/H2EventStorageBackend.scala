// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import java.sql.Connection

import com.daml.ledger.offset.Offset
import com.daml.platform.store.backend.common.{
  EventStorageBackendTemplate,
  ParameterStorageBackendTemplate,
}
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interning.StringInterning

class H2EventStorageBackend(ledgerEndCache: LedgerEndCache, stringInterning: StringInterning)
    extends EventStorageBackendTemplate(
      queryStrategy = H2QueryStrategy,
      eventStrategy = H2EventStrategy,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
      participantAllDivulgedContractsPrunedUpToInclusive =
        ParameterStorageBackendTemplate.participantAllDivulgedContractsPrunedUpToInclusive,
    ) {

  // Migration from mutable schema is not supported for H2
  override def isPruningOffsetValidAgainstMigration(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      connection: Connection,
  ): Boolean = true
}
