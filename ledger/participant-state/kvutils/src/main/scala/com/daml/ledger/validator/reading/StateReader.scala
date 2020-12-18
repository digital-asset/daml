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
trait StateReader[-Key, +Value] {
  self =>

  /**
    * Reads values of a set of keys from the backing store.
    *
    * @param keys list of keys to look up
    * @return values corresponding to the requested keys, in the same order as requested
    */
  def read(keys: Iterable[Key])(implicit executionContext: ExecutionContext): Future[Seq[Value]]

  /**
    * Create a new StateReader that transforms the keys before reading.
    *
    * This is an instance of a "contravariant functor", in which the mapping is backwards, because
    * it's upon input, not upon output.
    *
    * @param f the transformation function
    * @tparam NewKey the type of the keys
    * @return a `StateReader[NewKey, Value]`
    */
  def contramapKeys[NewKey](f: NewKey => Key): StateReader[NewKey, Value] =
    new StateReader[NewKey, Value] {
      override def read(
          keys: Iterable[NewKey]
      )(implicit executionContext: ExecutionContext): Future[Seq[Value]] =
        self.read(keys.map(f))
    }

  /**
    * Create a new StateReader that transforms the values after reading.
    *
    * @param f the transformation function
    * @tparam NewValue the type of the values
    * @return a `StateReader[Key, NewValue]`
    */
  def mapValues[NewValue](f: Value => NewValue): StateReader[Key, NewValue] =
    new StateReader[Key, NewValue] {
      override def read(
          keys: Iterable[Key]
      )(implicit executionContext: ExecutionContext): Future[Seq[NewValue]] =
        self.read(keys).map(_.map(f))
    }
}
