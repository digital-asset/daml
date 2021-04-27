// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.sql.Connection

import com.daml.ledger.participant.state.v1.Offset

trait StorageBackend[DB_BATCH] {
  def batch(dbDtos: Vector[DBDTOV1]): DB_BATCH
  def insertBatch(connection: Connection, batch: DB_BATCH): Unit
  def updateParams(connection: Connection, params: StorageBackend.Params): Unit
  def initialize(connection: Connection): StorageBackend.Initialized
}

object StorageBackend {
  case class Params(ledgerEnd: Offset, eventSeqId: Long, configuration: Option[Array[Byte]])

  case class Initialized(lastOffset: Option[Offset], lastEventSeqId: Option[Long])
}
