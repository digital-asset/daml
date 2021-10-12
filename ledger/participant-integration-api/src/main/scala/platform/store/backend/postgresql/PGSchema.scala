// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.DbDto
import com.daml.platform.store.backend.common.AppendOnlySchema.FieldStrategy
import com.daml.platform.store.backend.common.{AppendOnlySchema, Field, Schema, Table}
import com.daml.platform.store.cache.StringInterning

private[postgresql] object PGSchema {
  private val PGFieldStrategy = new FieldStrategy {
    override def stringArray[FROM, _](
        extractor: StringInterning => FROM => Iterable[String]
    ): Field[FROM, Iterable[String], _] =
      PGStringArray(extractor)

    override def stringArrayOptional[FROM, _](
        extractor: StringInterning => FROM => Option[Iterable[String]]
    ): Field[FROM, Option[Iterable[String]], _] =
      PGStringArrayOptional(extractor)

    override def intArray[FROM, _](
        extractor: StringInterning => FROM => Iterable[Int]
    ): Field[FROM, Iterable[Int], _] =
      PGIntArray(extractor)

    override def intArrayOptional[FROM, _](
        extractor: StringInterning => FROM => Option[Iterable[Int]]
    ): Field[FROM, Option[Iterable[Int]], _] =
      PGIntArrayOptional(extractor)

    override def smallintOptional[FROM, _](
        extractor: StringInterning => FROM => Option[Int]
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
