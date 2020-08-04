// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.validator.SubmissionValidator.RawKeyValuePairs

/**
  * A key value pair with the log entry.
  */
case class RawKeyValuePairsWithLogEntry(logEntry: (Bytes, Bytes), state: RawKeyValuePairs)
