// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.Fingerprint
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.reading.StateReader
import com.daml.lf.data.Time.Timestamp

package object preexecution {

  /**
    * Produces the record time on the participant for updates originating from pre-executed submissions.
    */
  type TimeUpdatesProvider =
    () => Option[Timestamp]

  type LedgerStateReaderWithFingerprints =
    StateReader[Key, (Option[Value], Fingerprint)]

  type DamlLedgerStateReaderWithFingerprints =
    StateReader[DamlStateKey, (Option[DamlStateValue], Fingerprint)]
}
