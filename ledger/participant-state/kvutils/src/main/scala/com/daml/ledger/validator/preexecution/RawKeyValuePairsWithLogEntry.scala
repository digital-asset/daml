// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.Bytes

/**
  * Raw key-value pairs with a distinct log entry.
  */
case class RawKeyValuePairsWithLogEntry(
    state: Iterable[(Bytes, Bytes)],
    logEntryKey: Bytes,
    logEntryValue: Bytes,
)
