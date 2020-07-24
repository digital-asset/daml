// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution
import com.daml.ledger.participant.state.kvutils.Fingerprint
import com.daml.ledger.validator.LedgerStateOperations
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.concurrent.{ExecutionContext, Future}

class LedgerReaderWithFingerprints[LogResult](
    ledgerStateOperations: LedgerStateOperations[LogResult],
    valueToFingerprint: Option[Value] => Fingerprint)(implicit executionContext: ExecutionContext)
    extends LedgerStateReaderWithFingerprints {

  override def read(keys: Seq[Key]): Future[Seq[(Option[Value], Fingerprint)]] =
    for {
      readResult <- ledgerStateOperations.readState(keys)
    } yield
      for {
        maybeValue <- readResult
      } yield maybeValue -> valueToFingerprint(maybeValue)
}
