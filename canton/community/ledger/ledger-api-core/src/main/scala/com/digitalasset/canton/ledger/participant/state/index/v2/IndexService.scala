// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import com.digitalasset.canton.ledger.api.health.ReportsHealth

trait IndexService
    extends IndexPackagesService
    with IndexConfigurationService
    with IndexCompletionsService
    with IndexTransactionsService
    with IndexEventQueryService
    with IndexActiveContractsService
    with ContractStore
    with MaximumLedgerTimeService
    with IdentityProvider
    with IndexPartyManagementService
    with IndexConfigManagementService
    with IndexParticipantPruningService
    with MeteringStore
    with ReportsHealth
