// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.messages.command.completion

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain.{ApplicationId, LedgerId, LedgerOffset}

case class CompletionStreamRequest(
    ledgerId: LedgerId,
    applicationId: ApplicationId,
    parties: Set[Ref.Party],
    offset: LedgerOffset
)
