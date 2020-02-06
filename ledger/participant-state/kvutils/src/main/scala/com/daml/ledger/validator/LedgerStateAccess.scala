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

  def writeState(key: Array[Byte], value: Array[Byte]): Future[Unit] = writeState(Seq((key, value)))

  def writeState(keyValuePairs: Seq[(Array[Byte], Array[Byte])]): Future[Unit]

  def appendToLog(key: Array[Byte], value: Array[Byte]): Future[Unit]
}

abstract class NonBatchingLedgerStateOperations(implicit executionContext: ExecutionContext)
    extends LedgerStateOperations {
  override def writeState(keyValuePairs: Seq[(Array[Byte], Array[Byte])]): Future[Unit] =
    Future
      .sequence(keyValuePairs.map {
        case (key, value) => writeState(key, value)
      })
      .map(_ => ())
}
