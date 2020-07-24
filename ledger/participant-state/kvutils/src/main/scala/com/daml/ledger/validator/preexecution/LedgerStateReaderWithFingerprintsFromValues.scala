// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution
import com.daml.ledger.participant.state.kvutils.Fingerprint
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.LedgerStateAccess

import scala.concurrent.{ExecutionContext, Future}

/**
  * An ledger state reader that also returns value fingerprints based on their value.
  *
  * It is useful to implement pre-execution for ledgers providing [[com.daml.ledger.validator.LedgerStateOperations]],
  * i.e. that don't directly provide fingerprints along with read values.
  *
  * @param ledgerStateAccess Transactional access to operations that can access actual ledger storage as part of a
  *                          transaction. Only reads will be used.
  * @param valueToFingerprint The logic producing a [[Fingerprint]] given a value.
  * @param executionContext The execution context for [[ledgerStateAccess]].
  * @tparam LogResult Type of the offset used for a log entry by [[ledgerStateAccess]].
  */
class LedgerStateReaderWithFingerprintsFromValues[LogResult](
    ledgerStateAccess: LedgerStateAccess[LogResult],
    valueToFingerprint: Option[Value] => Fingerprint)(implicit executionContext: ExecutionContext)
    extends LedgerStateReaderWithFingerprints {

  override def read(keys: Seq[Key]): Future[Seq[(Option[Value], Fingerprint)]] =
    for {
      readResult <- ledgerStateAccess.inTransaction { ledgerStateOperations =>
        ledgerStateOperations.readState(keys)
      }
    } yield
      for {
        maybeValue <- readResult
      } yield maybeValue -> valueToFingerprint(maybeValue)
}
