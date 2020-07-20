// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution
import com.daml.ledger.participant.state.kvutils.Fingerprint
import com.daml.ledger.validator.LedgerStateAccess
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.concurrent.{ExecutionContext, Future}

class LedgerStateAccessReaderWithFingerprints[LogResult](
    ledgerStateAccess: LedgerStateAccess[LogResult],
    valueToFingerprint: Option[Value] => Fingerprint)(implicit executionContext: ExecutionContext)
    extends LedgerStateReaderWithFingerprints {

  override def read(keys: Seq[Key]): Future[Seq[(Option[Value], Fingerprint)]] =
    ledgerStateAccess.inTransaction { state =>
      for {
        readResult <- state.readState(keys)
      } yield
        for {
          maybeValue <- readResult
        } yield maybeValue -> valueToFingerprint(maybeValue)
    }
}
