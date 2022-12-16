// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.transaction

import com.daml.lf.data.Ref.Party
import com.daml.lf.value.Value.ContractId

case class GetEventsByContractIdRequest(
    contractId: ContractId,
    requestingParties: Set[Party],
)
