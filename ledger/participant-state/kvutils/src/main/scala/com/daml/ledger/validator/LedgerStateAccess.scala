// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import scala.concurrent.{ExecutionContext, Future}

trait LedgerStateAccess {
  def inTransaction[T](body: LedgerStateOperations => Future[T]): Future[T]

  def participantId: String
}

trait LedgerStateOperations {
  type Key = Array[Byte]
  type Value = Array[Byte]

  def readState(key: Key): Future[Option[Value]]
  def readState(keys: Seq[Key]): Future[Seq[Option[Value]]]

  def writeState(key: Key, value: Value): Future[Unit]
  def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit]

  def appendToLog(key: Key, value: Value): Future[Unit]
}

abstract class BatchingLedgerStateOperations(implicit executionContext: ExecutionContext)
    extends LedgerStateOperations {
  override def readState(key: Key): Future[Option[Value]] =
    readState(Seq(key)).map(_.head)

  override def writeState(key: Key, value: Value): Future[Unit] =
    writeState(Seq((key, value)))
}

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
