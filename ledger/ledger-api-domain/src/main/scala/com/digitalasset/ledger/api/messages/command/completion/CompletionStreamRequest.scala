// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.command.completion

import com.daml.lf.data.Ref
import com.daml.ledger.api.domain.{ApplicationId, LedgerId, LedgerOffset}

case class CompletionStreamRequest(
    ledgerId: LedgerId,
    applicationId: ApplicationId,
    parties: Set[Ref.Party],
    offset: Option[LedgerOffset]
)
