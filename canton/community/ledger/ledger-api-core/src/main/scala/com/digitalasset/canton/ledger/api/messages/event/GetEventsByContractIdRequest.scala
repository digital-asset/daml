// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.event

import com.daml.lf.data.Ref.Party
import com.daml.lf.value.Value.ContractId

final case class GetEventsByContractIdRequest(
    contractId: ContractId,
    requestingParties: Set[Party],
)
