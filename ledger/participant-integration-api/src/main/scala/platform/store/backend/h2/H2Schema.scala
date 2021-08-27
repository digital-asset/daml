// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import com.daml.platform.store.backend.DbDto
import com.daml.platform.store.backend.common.AppendOnlySchema.FieldStrategy
import com.daml.platform.store.backend.common.{AppendOnlySchema, Field, Schema, Table}

private[h2] object H2Schema {
  private val H2FieldStrategy = new FieldStrategy {
    override def insert[FROM](tableName: String)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      Table.batchedInsert(tableName)(fields: _*)

    override def delete[FROM](tableName: String)(field: (String, Field[FROM, _, _])): Table[FROM] =
      Table.batchedDelete(tableName)(field)

    override def idempotentInsert[FROM](tableName: String, keyFieldIndex: Int)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      H2Table.idempotentBatchedInsert(tableName, keyFieldIndex)(fields: _*)
  }

  val schema: Schema[DbDto] = AppendOnlySchema(H2FieldStrategy)
}
