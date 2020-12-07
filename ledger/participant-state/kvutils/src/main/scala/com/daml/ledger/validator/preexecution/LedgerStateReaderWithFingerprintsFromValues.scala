// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution
import com.daml.ledger.participant.state.kvutils.Fingerprint
import com.daml.ledger.validator.LedgerStateOperations
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.concurrent.{ExecutionContext, Future}

/**
  * A ledger state reader that also generates value fingerprints based on their value.
  *
  * It is useful to implement pre-execution for ledgers providing [[LedgerStateOperations]],
  * i.e. that don't directly provide fingerprints along with read values.
  *
  * @param ledgerStateOperations The operations that can access actual ledger storage as part of a transaction.
  *                              Only reads will be used.
  * @param valueToFingerprint The logic producing a [[Fingerprint]] given a value.
  * @param executionContext The execution context for [[ledgerStateOperations]].
  * @tparam LogResult Type of the offset used for a log entry by [[ledgerStateOperations]].
  */
class LedgerStateReaderWithFingerprintsFromValues[LogResult](
    ledgerStateOperations: LedgerStateOperations[LogResult],
    valueToFingerprint: Option[Value] => Fingerprint)(implicit executionContext: ExecutionContext)
    extends LedgerStateReaderWithFingerprints {

  override def read(keys: Seq[Key], validateCached: Seq[(Key, Fingerprint)]): Future[(Seq[(Option[Value], Fingerprint)], Seq[(Key, (Option[Value], Fingerprint))])] =
    for {
      readResult <- ledgerStateOperations.readState(keys)
    } yield (readResult.map(maybeValue => maybeValue -> valueToFingerprint(maybeValue)), Seq.empty[(Key, (Option[Value], Fingerprint))])
}
