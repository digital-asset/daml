// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}

import scala.concurrent.{ExecutionContext, Future}

/** Defines how the validator/committer can access the backing store of the ledger to perform
  * read/write operations in a transaction.
  *
  * @tparam LogResult type of the offset used for a log entry
  */
trait LedgerStateAccess[+LogResult] {

  /** Performs read and write operations on the backing store in a single atomic transaction.
    *
    * @param body operations to perform
    * @tparam T return type of the body
    */
  def inTransaction[T](
      body: LedgerStateOperations[LogResult] => Future[T]
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[T]
}

/** Defines how the validator/committer can read from the backing store of the ledger.
  */
trait LedgerStateReadOperations {

  /** Reads value of a single key from the backing store.
    *
    * @param key key to look up data for
    * @return value corresponding to requested key or None in case it does not exist
    */
  def readState(
      key: Raw.StateKey
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Option[Raw.Envelope]]

  /** Reads values of a set of keys from the backing store.
    *
    * @param keys list of keys to look up data for
    * @return values corresponding to the requested keys, in the same order as requested
    */
  def readState(
      keys: Iterable[Raw.StateKey]
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Seq[Option[Raw.Envelope]]]
}

/** Defines how the validator/committer can write to the backing store of the ledger.
  *
  * @tparam LogResult type of the offset used for a log entry
  */
trait LedgerStateWriteOperations[+LogResult] {

  /** Writes a single key-value pair to the backing store.  In case the key already exists its value is overwritten.
    */
  def writeState(
      key: Raw.StateKey,
      value: Raw.Envelope,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit]

  /** Writes a list of key-value pairs to the backing store.  In case a key already exists its value is overwritten.
    */
  def writeState(
      keyValuePairs: Iterable[Raw.StateEntry]
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit]

  /** Writes a single log entry to the backing store.  The implementation may return Future.failed in case the key
    * (i.e., the log entry ID) already exists.
    *
    * @return offset of the latest log entry
    */
  def appendToLog(
      key: Raw.LogEntryId,
      value: Raw.Envelope,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[LogResult]
}

/** Defines how the validator/committer can access the backing store of the ledger.
  *
  * @tparam LogResult type of the offset used for a log entry
  */
trait LedgerStateOperations[+LogResult]
    extends LedgerStateReadOperations
    with LedgerStateWriteOperations[LogResult]

/** Convenience class for implementing read and write operations on a backing store that supports batched operations.
  */
abstract class BatchingLedgerStateOperations[LogResult] extends LedgerStateOperations[LogResult] {
  override final def readState(
      key: Raw.StateKey
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Option[Raw.Envelope]] =
    readState(Seq(key)).map(_.head)(ExecutionContext.parasitic)

  override final def writeState(
      key: Raw.StateKey,
      value: Raw.Envelope,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] =
    writeState(Seq(key -> value))
}

/** Convenience class for implementing read and write operations on a backing store that '''does not''' support batched
  * operations.
  */
abstract class NonBatchingLedgerStateOperations[LogResult]
    extends LedgerStateOperations[LogResult] {
  override final def readState(
      keys: Iterable[Raw.StateKey]
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Seq[Option[Raw.Envelope]]] =
    Future.sequence(keys.map(readState)).map(_.toSeq)

  override final def writeState(
      keyValuePairs: Iterable[Raw.StateEntry]
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] =
    Future
      .sequence(keyValuePairs.map { case (key, value) =>
        writeState(key, value)
      })
      .map(_ => ())
}

final class TimedLedgerStateOperations[LogResult](
    delegate: LedgerStateOperations[LogResult],
    metrics: Metrics,
) extends LedgerStateOperations[LogResult] {

  override def readState(
      key: Raw.StateKey
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Option[Raw.Envelope]] =
    Timed.future(metrics.daml.ledger.state.read, delegate.readState(key))

  override def readState(
      keys: Iterable[Raw.StateKey]
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Seq[Option[Raw.Envelope]]] =
    Timed.future(metrics.daml.ledger.state.read, delegate.readState(keys))

  override def writeState(
      key: Raw.StateKey,
      value: Raw.Envelope,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] =
    Timed.future(metrics.daml.ledger.state.write, delegate.writeState(key, value))

  override def writeState(
      keyValuePairs: Iterable[Raw.StateEntry]
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] =
    Timed.future(metrics.daml.ledger.state.write, delegate.writeState(keyValuePairs))

  override def appendToLog(
      key: Raw.LogEntryId,
      value: Raw.Envelope,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[LogResult] =
    Timed.future(metrics.daml.ledger.log.append, delegate.appendToLog(key, value))
}
