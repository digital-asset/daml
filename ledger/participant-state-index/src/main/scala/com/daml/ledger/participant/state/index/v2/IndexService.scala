// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

trait IndexService
    extends IndexPackagesService
    with IndexConfigurationService
    with IndexCompletionsService
    with IndexTransactionsService
    with IndexActiveContractsService
    with ContractStore
    with IdentityProvider
//with IndexTimeService //TODO: this needs some further discussion as the TimeService is actually optional
