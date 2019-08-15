// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1

trait IndexService
    extends PackagesService
    with ConfigurationService
    with CompletionsService
    with TransactionsService
    with ActiveContractsService
    with ContractStore
    with IdentityService
    with TimeService
    with PartyManagementService
