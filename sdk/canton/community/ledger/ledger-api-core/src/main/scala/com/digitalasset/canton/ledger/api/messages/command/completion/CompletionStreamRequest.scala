// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.command.completion

import com.digitalasset.canton.ledger.api.domain.ParticipantOffset
import com.digitalasset.daml.lf.data.Ref

final case class CompletionStreamRequest(
    applicationId: Ref.ApplicationId,
    parties: Set[Ref.Party],
    offset: Option[ParticipantOffset],
)
