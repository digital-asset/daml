// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.digitalasset.ledger.api.health.ReportsHealth

trait IndexService
    extends IndexPackagesService
    with IndexConfigurationService
    with IndexCompletionsService
    with IndexTransactionsService
    with IndexActiveContractsService
    with ContractStore
    with IdentityProvider
    with IndexPartyManagementService
    with IndexConfigManagementService
    with IndexSubmissionService
    // with IndexTimeService //TODO: this needs some further discussion as the TimeService is actually optional
    with ReportsHealth
