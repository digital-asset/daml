// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import scala.concurrent.{ExecutionContext, Future}

trait LedgerStateAccess {

  /**
    * Performs read and write operations on the backing store in a single atomic transaction.
    * @param body  operations to perform
    * @tparam T type of result returned after execution
    */
  def inTransaction[T](body: LedgerStateOperations => Future[T]): Future[T]

  /**
    * @return  participant's ID from which the backing store is being accessed from
    */
  def participantId: String
}

trait LedgerStateOperations {
  type Key = Array[Byte]
  type Value = Array[Byte]

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
    */
  def appendToLog(key: Key, value: Value): Future[Unit]
}

/**
  * Implements non-batching read and write operations on the backing store based on batched implementations.
  */
abstract class BatchingLedgerStateOperations(implicit executionContext: ExecutionContext)
    extends LedgerStateOperations {
  override def readState(key: Key): Future[Option[Value]] =
    readState(Seq(key)).map(_.head)

  override def writeState(key: Key, value: Value): Future[Unit] =
    writeState(Seq((key, value)))
}

/**
  * Implements batching read and write operations on the backing store based on non-batched implementations.
  */
abstract class NonBatchingLedgerStateOperations(implicit executionContext: ExecutionContext)
    extends LedgerStateOperations {
  override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
    Future.sequence(keys.map(readState))

  override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
    Future
      .sequence(keyValuePairs.map {
        case (key, value) => writeState(key, value)
      })
      .map(_ => ())
}
