// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.event

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.value.Value

final case class GetEventsByContractKeyRequest(
    contractKey: Value,
    templateId: Ref.Identifier,
    requestingParties: Set[Party],
    endExclusiveSeqId: Option[Long],
)
