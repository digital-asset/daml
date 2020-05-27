// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.concurrent.Future

trait LedgerStateReader {

  /**
    * Reads values of a set of keys from the backing store.
    * @param keys  list of keys to look up data for
    * @return  values corresponding to the requested keys, in the same order as requested
    */
  def read(keys: Seq[Key]): Future[Seq[Option[Value]]]
}
