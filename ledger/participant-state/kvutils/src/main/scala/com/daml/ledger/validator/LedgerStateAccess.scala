// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.validator.LedgerStateOperations._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Defines how the validator/committer can access the backing store of the ledger to perform read/write operations in
  * a transaction.
  * @tparam LogResult type of the offset used for a log entry
  */
trait LedgerStateAccess[LogResult] {

  /**
    * Performs read and write operations on the backing store in a single atomic transaction.
    *
    * @param body  operations to perform
    * @tparam T return type of the body
    */
  def inTransaction[T](body: LedgerStateOperations[LogResult] => Future[T]): Future[T]
}

/**
  * Defines how the validator/committer can access the backing store of the ledger.
  * @tparam LogResult type of the offset used for a log entry
  */
trait LedgerStateOperations[LogResult] {

  /**
    * Reads value of a single key from the backing store.
    * @param key  key to look up data for
    * @return value corresponding to requested key or None in case it does not exist
    */
  def readState(key: Key): Future[Option[Value]]

  /**
    * Reads values of a set of keys from the backing store.
    * @param keys  list of keys to look up data for
    * @return  values corresponding to the requested keys, in the same order as requested
    */
  def readState(keys: Seq[Key]): Future[Seq[Option[Value]]]

  /**
    * Writes a single key-value pair to the backing store.  In case the key already exists its value is overwritten.
    */
  def writeState(key: Key, value: Value): Future[Unit]

  /**
    * Writes a list of key-value pairs to the backing store.  In case a key already exists its value is overwritten.
    */
  def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit]

  /**
    * Writes a single log entry to the backing store.  The implementation may return Future.failed in case the key
    * (i.e., the log entry ID) already exists.
    * @return  offset of the latest log entry
    */
  def appendToLog(key: Key, value: Value): Future[LogResult]
}

/**
  * Provides default implementations for non-batching read and write operations based on batched operations on the
  * backing store.
  * You should extend this class in case your backing store supports batched operations.
  */
abstract class BatchingLedgerStateOperations[LogResult](implicit executionContext: ExecutionContext)
    extends LedgerStateOperations[LogResult] {
  override final def readState(key: Key): Future[Option[Value]] =
    readState(Seq(key)).map(_.head)

  override final def writeState(key: Key, value: Value): Future[Unit] =
    writeState(Seq(key -> value))
}

/**
  * Provides default implementations for batching read and write operations based on non-batched operations on the
  * backing store.
  * You should extend this class in case your backing store does not support batched operations.
  */
abstract class NonBatchingLedgerStateOperations[LogResult](
    implicit executionContext: ExecutionContext
) extends LedgerStateOperations[LogResult] {
  override final def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
    Future.sequence(keys.map(readState))

  override final def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
    Future
      .sequence(keyValuePairs.map {
        case (key, value) => writeState(key, value)
      })
      .map(_ => ())
}

object LedgerStateOperations {
  type Key = Bytes
  type Value = Bytes
}
