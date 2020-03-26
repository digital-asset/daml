// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerWriter}
import com.daml.ledger.participant.state.v1.{ReadService, WriteService}

package object app {
  type KeyValueLedger = LedgerReader with LedgerWriter
  type ReadWriteService = ReadService with WriteService
}
