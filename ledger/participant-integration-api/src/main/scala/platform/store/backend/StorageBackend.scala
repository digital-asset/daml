// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection

import com.daml.ledger.participant.state.v1.Offset
import com.daml.platform.store.DbType
import com.daml.platform.store.backend.postgresql.PostgresStorageBackend

trait StorageBackend[DB_BATCH] {
  def batch(dbDtos: Vector[DBDTOV1]): DB_BATCH
  def insertBatch(connection: Connection, batch: DB_BATCH): Unit
  def updateParams(connection: Connection, params: StorageBackend.Params): Unit
  def initialize(connection: Connection): StorageBackend.Initialized
}

object StorageBackend {
  case class Params(ledgerEnd: Offset, eventSeqId: Long, configuration: Option[Array[Byte]])

  case class Initialized(lastOffset: Option[Offset], lastEventSeqId: Option[Long])

  def of(dbType: DbType): StorageBackend[_] =
    dbType match {
      case DbType.H2Database => throw new UnsupportedOperationException("H2 not supported yet")
      case DbType.Postgres => PostgresStorageBackend
      case DbType.Oracle => throw new UnsupportedOperationException("Oracle not supported yet")
    }
}
