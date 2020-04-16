// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.validator.LedgerStateOperations._
import com.daml.metrics.{MetricName, Timed}

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
  * Convenience class for implementing read and write operations on a backing store that supports batched operations.
  */
abstract class BatchingLedgerStateOperations[LogResult](implicit executionContext: ExecutionContext)
    extends LedgerStateOperations[LogResult] {
  override final def readState(key: Key): Future[Option[Value]] =
    readState(Seq(key)).map(_.head)

  override final def writeState(key: Key, value: Value): Future[Unit] =
    writeState(Seq(key -> value))
}

/**
  * Convenience class for implementing read and write operations on a backing store that '''does not''' support batched
  * operations.
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

final class TimedLedgerStateOperations[LogResult](
    delegate: LedgerStateOperations[LogResult],
    metricRegistry: MetricRegistry,
) extends LedgerStateOperations[LogResult] {
  override def readState(key: Key): Future[Option[Value]] =
    Timed.future(Metrics.readState, delegate.readState(key))

  override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
    Timed.future(Metrics.readState, delegate.readState(keys))

  override def writeState(key: Key, value: Value): Future[Unit] =
    Timed.future(Metrics.writeState, delegate.writeState(key, value))

  override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
    Timed.future(Metrics.writeState, delegate.writeState(keyValuePairs))

  override def appendToLog(key: Key, value: Value): Future[LogResult] =
    Timed.future(Metrics.appendToLog, delegate.appendToLog(key, value))

  private object Metrics {
    val appendToLog: Timer = metricRegistry.timer(MetricPrefix :+ "log" :+ "append")
    val readState: Timer = metricRegistry.timer(MetricPrefix :+ "state" :+ "read")
    val writeState: Timer = metricRegistry.timer(MetricPrefix :+ "state" :+ "write")
  }
}

object LedgerStateOperations {
  type Key = Bytes
  type Value = Bytes

  val MetricPrefix: MetricName = MetricName.DAML :+ "ledger"
}
