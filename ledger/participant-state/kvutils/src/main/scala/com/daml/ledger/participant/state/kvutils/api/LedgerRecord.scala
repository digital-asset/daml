// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.Raw

/** A log entry read from the ledger.
  *
  * @param offset   offset of log entry
  * @param entryId  opaque ID of log entry
  * @param envelope opaque contents of log entry
  */
final case class LedgerRecord(offset: Offset, entryId: Raw.LogEntryId, envelope: Raw.Envelope)
