// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import com.daml.platform.store.backend.DbDto
import com.daml.platform.store.backend.common.AppendOnlySchema.FieldStrategy
import com.daml.platform.store.backend.common.{AppendOnlySchema, Field, Schema, Table}
import com.daml.platform.store.interning.StringInterning

private[oracle] object OracleSchema {
  private val OracleFieldStrategy = new FieldStrategy {
    override def stringArrayOptional[FROM](
        extractor: StringInterning => FROM => Option[Iterable[String]]
    ): Field[FROM, Option[Iterable[String]], _] =
      OracleStringArrayOptional(extractor)

    override def intArray[FROM](
        extractor: StringInterning => FROM => Iterable[Int]
    ): Field[FROM, Iterable[Int], _] =
      OracleIntArray(extractor)

    override def intArrayOptional[FROM](
        extractor: StringInterning => FROM => Option[Iterable[Int]]
    ): Field[FROM, Option[Iterable[Int]], _] =
      OracleIntArrayOptional(extractor)

    override def insert[FROM](tableName: String)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      Table.batchedInsert(tableName)(fields: _*)

    override def idempotentInsert[FROM](tableName: String, keyFieldIndex: Int)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      OracleTable.idempotentInsert(tableName, keyFieldIndex)(fields: _*)
  }

  val schema: Schema[DbDto] = AppendOnlySchema(OracleFieldStrategy)
}
