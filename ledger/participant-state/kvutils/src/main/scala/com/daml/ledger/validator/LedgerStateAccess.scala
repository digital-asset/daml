// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.validator.LedgerStateOperations._
import com.daml.metrics.{Metrics, Timed}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Defines how the validator/committer can access the backing store of the ledger to perform
  * read/write operations in a transaction.
  *
  * @tparam LogResult type of the offset used for a log entry
  */
trait LedgerStateAccess[LogResult] {

  /**
    * Performs read and write operations on the backing store in a single atomic transaction.
    *
    * @param body operations to perform
    * @tparam T return type of the body
    */
  def inTransaction[T](
      body: LedgerStateOperations[LogResult] => Future[T],
  )(implicit executionContext: ExecutionContext): Future[T]
}

/**
  * Defines how the validator/committer can access the backing store of the ledger.
  *
  * @tparam LogResult type of the offset used for a log entry
  */
trait LedgerStateOperations[+LogResult] {

  /**
    * Reads value of a single key from the backing store.
    *
    * @param key key to look up data for
    * @return value corresponding to requested key or None in case it does not exist
    */
  def readState(key: Key)(implicit executionContext: ExecutionContext): Future[Option[Value]]

  /**
    * Reads values of a set of keys from the backing store.
    *
    * @param keys list of keys to look up data for
    * @return values corresponding to the requested keys, in the same order as requested
    */
  def readState(
      keys: Iterable[Key],
  )(implicit executionContext: ExecutionContext): Future[Seq[Option[Value]]]

  /**
    * Writes a single key-value pair to the backing store.  In case the key already exists its value is overwritten.
    */
  def writeState(
      key: Key,
      value: Value,
  )(implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * Writes a list of key-value pairs to the backing store.  In case a key already exists its value is overwritten.
    */
  def writeState(
      keyValuePairs: Iterable[(Key, Value)],
  )(implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * Writes a single log entry to the backing store.  The implementation may return Future.failed in case the key
    * (i.e., the log entry ID) already exists.
    *
    * @return offset of the latest log entry
    */
  def appendToLog(
      key: Key,
      value: Value,
  )(implicit executionContext: ExecutionContext): Future[LogResult]
}

/**
  * Convenience class for implementing read and write operations on a backing store that supports batched operations.
  */
abstract class BatchingLedgerStateOperations[LogResult] extends LedgerStateOperations[LogResult] {
  override final def readState(
      key: Key,
  )(implicit executionContext: ExecutionContext): Future[Option[Value]] =
    readState(Seq(key)).map(_.head)(DirectExecutionContext)

  override final def writeState(
      key: Key,
      value: Value,
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    writeState(Seq(key -> value))
}

/**
  * Convenience class for implementing read and write operations on a backing store that '''does not''' support batched
  * operations.
  */
abstract class NonBatchingLedgerStateOperations[LogResult]
    extends LedgerStateOperations[LogResult] {
  override final def readState(
      keys: Iterable[Key],
  )(implicit executionContext: ExecutionContext): Future[Seq[Option[Value]]] =
    Future.sequence(keys.map(readState)).map(_.toSeq)

  override final def writeState(
      keyValuePairs: Iterable[(Key, Value)],
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    Future
      .sequence(keyValuePairs.map {
        case (key, value) => writeState(key, value)
      })
      .map(_ => ())
}

final class TimedLedgerStateOperations[LogResult](
    delegate: LedgerStateOperations[LogResult],
    metrics: Metrics,
) extends LedgerStateOperations[LogResult] {

  override def readState(
      key: Key,
  )(implicit executionContext: ExecutionContext): Future[Option[Value]] =
    Timed.future(metrics.daml.ledger.state.read, delegate.readState(key))

  override def readState(
      keys: Iterable[Key],
  )(implicit executionContext: ExecutionContext): Future[Seq[Option[Value]]] =
    Timed.future(metrics.daml.ledger.state.read, delegate.readState(keys))

  override def writeState(
      key: Key,
      value: Value,
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    Timed.future(metrics.daml.ledger.state.write, delegate.writeState(key, value))

  override def writeState(
      keyValuePairs: Iterable[(Key, Value)],
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    Timed.future(metrics.daml.ledger.state.write, delegate.writeState(keyValuePairs))

  override def appendToLog(
      key: Key,
      value: Value,
  )(implicit executionContext: ExecutionContext): Future[LogResult] =
    Timed.future(metrics.daml.ledger.log.append, delegate.appendToLog(key, value))
}

object LedgerStateOperations {

  type Key = Bytes
  type Value = Bytes

}
