// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.api.health.ReportsHealth

trait IndexService
    extends IndexPackagesService
    with IndexConfigurationService
    with IndexCompletionsService
    with IndexTransactionsService
    with IndexActiveContractsService
    with ContractStore

    /** TDT, ideally this should not be exposed here, since it doesn't expose a service to the Ledger API */
    with IdentityProvider
    with IndexPartyManagementService
    with IndexConfigManagementService
    with IndexParticipantPruningService
    with IndexSubmissionService
    // with IndexTimeService //TODO: this needs some further discussion as the TimeService is actually optional
    with ReportsHealth
