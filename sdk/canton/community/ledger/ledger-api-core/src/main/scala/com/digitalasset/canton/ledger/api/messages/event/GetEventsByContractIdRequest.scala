// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.event

import com.digitalasset.canton.ledger.api.EventFormat
import com.digitalasset.daml.lf.value.Value.ContractId

final case class GetEventsByContractIdRequest(
    contractId: ContractId,
    eventFormat: EventFormat,
)
