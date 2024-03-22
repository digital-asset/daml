// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.oracle

import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.common.{
  EventStorageBackendTemplate,
  ParameterStorageBackendImpl,
}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning

import java.sql.Connection

class OracleEventStorageBackend(
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
    loggerFactory: NamedLoggerFactory,
) extends EventStorageBackendTemplate(
      queryStrategy = OracleQueryStrategy,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
      participantAllDivulgedContractsPrunedUpToInclusive =
        ParameterStorageBackendImpl.participantAllDivulgedContractsPrunedUpToInclusive,
      loggerFactory = loggerFactory,
    ) {

  // Migration from mutable schema is not supported for Oracle
  override def isPruningOffsetValidAgainstMigration(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      connection: Connection,
  ): Boolean = true

}
