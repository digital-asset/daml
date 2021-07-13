// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.command.completion

import com.daml.ledger.api.domain.LedgerId

case class CompletionEndRequest(ledgerId: LedgerId)
