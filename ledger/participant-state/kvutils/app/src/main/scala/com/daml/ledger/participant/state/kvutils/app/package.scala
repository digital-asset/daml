// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerWriter}

package object app {
  type KeyValueLedger = LedgerReader with LedgerWriter with AutoCloseable
}
