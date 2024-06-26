// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.oracle

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend
import com.digitalasset.canton.platform.store.backend.common.EventStorageBackendTemplate
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning

class OracleEventStorageBackend(
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
    parameterStorageBackend: ParameterStorageBackend,
    loggerFactory: NamedLoggerFactory,
) extends EventStorageBackendTemplate(
      queryStrategy = OracleQueryStrategy,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
      participantAllDivulgedContractsPrunedUpToInclusive =
        parameterStorageBackend.participantAllDivulgedContractsPrunedUpToInclusive,
      loggerFactory = loggerFactory,
    ) {}
