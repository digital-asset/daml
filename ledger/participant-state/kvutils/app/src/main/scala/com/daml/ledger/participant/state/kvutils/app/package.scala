// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerWriter}
import com.daml.ledger.participant.state.v1.{ReadService, WriteService}

package object app {
  type KeyValueLedger = LedgerReader with LedgerWriter
  type ReadWriterService = ReadService with WriteService
}
