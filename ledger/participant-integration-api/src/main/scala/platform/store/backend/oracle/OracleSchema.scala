// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import java.time.Instant

import com.daml.platform.store.backend.DbDto
import com.daml.platform.store.backend.common.AppendOnlySchema.FieldStrategy
import com.daml.platform.store.backend.common.{AppendOnlySchema, Field, Schema, Table}

private[oracle] object OracleSchema {
  private val OracleFieldStrategy = new FieldStrategy {
    override def stringArray[FROM, _](
        extractor: FROM => Iterable[String]
    ): Field[FROM, Iterable[String], _] =
      OracleStringArray(extractor)

    override def stringArrayOptional[FROM, _](
        extractor: FROM => Option[Iterable[String]]
    ): Field[FROM, Option[Iterable[String]], _] =
      OracleStringArrayOptional(extractor)

    override def timestamp[FROM, _](extractor: FROM => Instant): Field[FROM, Instant, _] =
      OracleTimestamp(extractor)

    override def timestampOptional[FROM, _](
        extractor: FROM => Option[Instant]
    ): Field[FROM, Option[Instant], _] =
      OracleTimestampOptional(extractor)

    override def insert[FROM](tableName: String)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      Table.batchedInsert(tableName)(fields: _*)

    override def delete[FROM](tableName: String)(field: (String, Field[FROM, _, _])): Table[FROM] =
      Table.batchedDelete(tableName)(field)

    override def idempotentInsert[FROM](tableName: String, keyFieldIndex: Int)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      OracleTable.idempotentBatchedInsert(tableName, keyFieldIndex)(fields: _*)
  }

  val schema: Schema[DbDto] = AppendOnlySchema(OracleFieldStrategy)
}
