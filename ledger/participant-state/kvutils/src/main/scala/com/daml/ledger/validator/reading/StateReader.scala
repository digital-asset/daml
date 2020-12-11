// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.reading

import scala.concurrent.{ExecutionContext, Future}

/**
  * Generic interface for reading from the ledger.
  *
  * @tparam Key   The type of the key expected.
  * @tparam Value The type of the value returned.
  */
trait StateReader[Key, Value] {

  /**
    * Reads values of a set of keys from the backing store.
    *
    * @param keys list of keys to look up
    * @return values corresponding to the requested keys, in the same order as requested
    */
  def read(keys: Seq[Key])(implicit executionContext: ExecutionContext): Future[Seq[Option[Value]]]
}
