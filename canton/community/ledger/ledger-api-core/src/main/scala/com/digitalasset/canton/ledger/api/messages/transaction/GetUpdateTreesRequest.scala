// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.transaction

import com.daml.lf.data.Ref.Party
import com.digitalasset.canton.ledger.api.domain.ParticipantOffset

final case class GetUpdateTreesRequest(
    startExclusive: ParticipantOffset,
    endInclusive: Option[ParticipantOffset],
    parties: Set[Party],
    verbose: Boolean,
)
