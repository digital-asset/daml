// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on

import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerWriter}

package object memory {
  type Index = Int

  type KeyValueLedger = LedgerReader with LedgerWriter
}
