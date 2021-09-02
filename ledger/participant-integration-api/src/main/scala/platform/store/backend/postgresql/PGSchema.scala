// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.DbDto
import com.daml.platform.store.backend.common.AppendOnlySchema.FieldStrategy
import com.daml.platform.store.backend.common.{AppendOnlySchema, Field, Schema, Table}

private[postgresql] object PGSchema {
  private val PGFieldStrategy = new FieldStrategy {
    override def stringArray[FROM, _](
        extractor: FROM => Iterable[String]
    ): Field[FROM, Iterable[String], _] =
      PGStringArray(extractor)

    override def stringArrayOptional[FROM, _](
        extractor: FROM => Option[Iterable[String]]
    ): Field[FROM, Option[Iterable[String]], _] =
      PGStringArrayOptional(extractor)

    override def smallintOptional[FROM, _](
        extractor: FROM => Option[Int]
    ): Field[FROM, Option[Int], _] =
      PGSmallintOptional(extractor)

    override def insert[FROM](tableName: String)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      PGTable.transposedInsert(tableName)(fields: _*)

    override def delete[FROM](tableName: String)(field: (String, Field[FROM, _, _])): Table[FROM] =
      PGTable.transposedDelete(tableName)(field)

    override def idempotentInsert[FROM](tableName: String, keyFieldIndex: Int)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      PGTable.idempotentTransposedInsert(tableName, keyFieldIndex)(fields: _*)
  }

  val schema: Schema[DbDto] = AppendOnlySchema(PGFieldStrategy)
}
