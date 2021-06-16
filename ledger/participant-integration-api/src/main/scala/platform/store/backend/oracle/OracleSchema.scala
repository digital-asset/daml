// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import com.daml.platform.store.backend.DbDto
import com.daml.platform.store.backend.common.AppendOnlySchema.FieldStrategy
import com.daml.platform.store.backend.common.{AppendOnlySchema, Field, Schema, Table}

private[oracle] object OracleSchema {
  private val OracleFieldStrategy = new FieldStrategy {
    override def insert[FROM](tableName: String)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      Table.batchedInsert(tableName)(fields: _*)

    override def delete[FROM](tableName: String)(field: (String, Field[FROM, _, _])): Table[FROM] =
      Table.batchedDelete(tableName)(field)

    override def idempotentInsert(tableName: String, keyFieldIndex: Int)(
        fields: (String, Field[DbDto.Package, _, _])*
    ): Table[DbDto.Package] =
      OracleTable.idempotentBatchedInsert(tableName, keyFieldIndex)(fields: _*)
  }

  val schema: Schema[DbDto] = AppendOnlySchema(OracleFieldStrategy)
}
