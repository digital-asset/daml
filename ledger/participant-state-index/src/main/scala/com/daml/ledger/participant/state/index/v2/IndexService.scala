// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

trait IndexService
    extends PackagesService
    with ConfigurationService
    with CompletionsService
    with TransactionsService
    with ActiveContractsService
    with ContractStore
    with IdentityService
    with TimeService
