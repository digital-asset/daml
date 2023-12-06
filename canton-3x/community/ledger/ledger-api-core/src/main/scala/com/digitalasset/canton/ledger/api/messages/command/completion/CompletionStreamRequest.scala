// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.command.completion

import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain.{LedgerId, LedgerOffset}

final case class CompletionStreamRequest(
    ledgerId: Option[LedgerId],
    applicationId: Ref.ApplicationId,
    parties: Set[Ref.Party],
    offset: Option[LedgerOffset],
)
