// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.event

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.value.Value.ContractId

final case class GetEventsByContractIdRequest(
    contractId: ContractId,
    requestingParties: Set[Party],
)
