// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.command.completion

import com.daml.ledger.api.domain.{LedgerId, LedgerOffset}
import com.daml.lf.data.Ref

case class CompletionStreamRequest(
    ledgerId: Option[LedgerId],
    applicationId: Ref.ApplicationId,
    parties: Set[Ref.Party],
    offset: Option[LedgerOffset],
)
