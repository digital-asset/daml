// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.transaction

import com.daml.ledger.api.domain.LedgerId

final case class GetLedgerEndRequest(ledgerId: LedgerId)
