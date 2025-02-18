// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.canton.ledger.api.health.ReportsHealth

trait IndexService
    extends IndexCompletionsService
    with IndexUpdateService
    with IndexEventQueryService
    with IndexActiveContractsService
    with ContractStore
    with MaximumLedgerTimeService
    with IndexPartyManagementService
    with IndexParticipantPruningService
    with MeteringStore
    with ReportsHealth
