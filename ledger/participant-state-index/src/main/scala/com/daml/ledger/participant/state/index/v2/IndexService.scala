// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

//TODO: rename all subtraits to contain "Index" prefix in a follow-up PR!
trait IndexService
    extends PackagesService
    with ConfigurationService
    with IndexCompletionsService
    with TransactionsService
    with ActiveContractsService
    with ContractStore
    with IdentityService
    with TimeService
