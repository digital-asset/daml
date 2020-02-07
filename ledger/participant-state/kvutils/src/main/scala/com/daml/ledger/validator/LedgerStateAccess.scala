// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import scala.concurrent.{ExecutionContext, Future}

trait LedgerStateAccess {
  def inTransaction[T](body: LedgerStateOperations => Future[T]): Future[T]

  def participantId: String
}

trait LedgerStateOperations {
  def readState(key: Array[Byte]): Future[Option[Array[Byte]]]
  def readState(keys: Seq[Array[Byte]]): Future[Seq[Option[Array[Byte]]]]

  def writeState(key: Array[Byte], value: Array[Byte]): Future[Unit]
  def writeState(keyValuePairs: Seq[(Array[Byte], Array[Byte])]): Future[Unit]

  def appendToLog(key: Array[Byte], value: Array[Byte]): Future[Unit]
}

abstract class BatchingLedgerStateOperations(implicit executionContext: ExecutionContext)
    extends LedgerStateOperations {
  override def readState(key: Array[Byte]): Future[Option[Array[Byte]]] =
    readState(Seq(key)).map(_.head)

  override def writeState(key: Array[Byte], value: Array[Byte]): Future[Unit] =
    writeState(Seq((key, value)))
}

abstract class NonBatchingLedgerStateOperations(implicit executionContext: ExecutionContext)
    extends LedgerStateOperations {
  override def readState(keys: Seq[Array[Byte]]): Future[Seq[Option[Array[Byte]]]] =
    Future.sequence(keys.map(readState))

  override def writeState(keyValuePairs: Seq[(Array[Byte], Array[Byte])]): Future[Unit] =
    Future
      .sequence(keyValuePairs.map {
        case (key, value) => writeState(key, value)
      })
      .map(_ => ())
}
