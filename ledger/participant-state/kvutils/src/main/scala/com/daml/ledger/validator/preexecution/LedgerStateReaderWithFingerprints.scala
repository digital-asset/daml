// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.Fingerprint
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.concurrent.Future

trait LedgerStateReaderWithFingerprints {

  /**
    * Reads values of a set of keys from the backing store, including their associated fingerprints.
    * @param keys  list of keys to look up data for
    * @return  fingerprinted results of key lookups for the requested keys, in the same order as requested
    */
  def read(keys: Seq[Key], validateCached: Seq[(Key, Fingerprint)]): Future[(Seq[(Option[Value], Fingerprint)], Seq[(Key, (Option[Value], Fingerprint))])]
}

trait DamlLedgerStateReaderWithFingerprints {
  def read(keys: Seq[DamlStateKey], validateCached: Seq[(DamlStateKey, Fingerprint)]): Future[Seq[(Option[DamlStateValue], Fingerprint)]]
}
