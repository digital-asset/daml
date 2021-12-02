// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.command.completion

import com.daml.ledger.api.domain.{ApplicationId, LedgerId, LedgerOffset}
import com.daml.lf.data.Ref

case class CompletionRequest(
    ledgerId: LedgerId,
    applicationId: ApplicationId,
    parties: Set[Ref.Party],
    offset: LedgerOffset.Absolute,
)
