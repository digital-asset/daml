// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import com.daml.platform.store.backend.DbDto
import com.daml.platform.store.backend.common.AppendOnlySchema.FieldStrategy
import com.daml.platform.store.backend.common.{AppendOnlySchema, Field, Schema, Table}
import com.daml.platform.store.interning.StringInterning

private[h2] object H2Schema {
  private val H2FieldStrategy = new FieldStrategy {
    override def intArray[FROM](
        extractor: StringInterning => FROM => Iterable[Int]
    ): Field[FROM, Iterable[Int], _] =
      IntArray(extractor)

    override def intArrayOptional[FROM](
        extractor: StringInterning => FROM => Option[Iterable[Int]]
    ): Field[FROM, Option[Iterable[Int]], _] =
      IntArrayOptional(extractor)

    override def bytea[FROM](
        extractor: StringInterning => FROM => Array[Byte]
    ): Field[FROM, Array[Byte], _] =
      H2Bytea(extractor)

    override def byteaOptional[FROM](
        extractor: StringInterning => FROM => Option[Array[Byte]]
    ): Field[FROM, Option[Array[Byte]], _] =
      H2ByteaOptional(extractor)

    override def insert[FROM](tableName: String)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      Table.batchedInsert(tableName)(fields: _*)

    override def idempotentInsert[FROM](tableName: String, keyFieldIndex: Int)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      H2Table.idempotentBatchedInsert(tableName, keyFieldIndex)(fields: _*)
  }

  val schema: Schema[DbDto] = AppendOnlySchema(H2FieldStrategy)
}
