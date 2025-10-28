// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.common.EventStorageBackendTemplate
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning

class PostgresEventStorageBackend(
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
    loggerFactory: NamedLoggerFactory,
) extends EventStorageBackendTemplate(
      queryStrategy = PostgresQueryStrategy,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
      loggerFactory = loggerFactory,
    )
