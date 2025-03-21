// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.command.completion

import com.digitalasset.canton.data.Offset
import com.digitalasset.daml.lf.data.Ref

final case class CompletionStreamRequest(
    userId: Ref.UserId,
    parties: Set[Ref.Party],
    offset: Option[Offset],
)
