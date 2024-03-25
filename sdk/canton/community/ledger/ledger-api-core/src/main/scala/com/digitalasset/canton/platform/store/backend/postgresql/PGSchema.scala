// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.platform.store.backend.DbDto
import com.digitalasset.canton.platform.store.backend.common.AppendOnlySchema.FieldStrategy
import com.digitalasset.canton.platform.store.backend.common.{
  AppendOnlySchema,
  Field,
  Schema,
  Table,
}
import com.digitalasset.canton.platform.store.interning.StringInterning

private[postgresql] object PGSchema {
  private val PGFieldStrategy = new FieldStrategy {
    override def stringArray[FROM](
        extractor: StringInterning => FROM => Iterable[String]
    ): Field[FROM, Iterable[String], _] =
      PGStringArray(extractor)

    override def intArray[FROM](
        extractor: StringInterning => FROM => Iterable[Int]
    ): Field[FROM, Iterable[Int], _] =
      PGIntArray(extractor)

    override def intArrayOptional[FROM](
        extractor: StringInterning => FROM => Option[Iterable[Int]]
    ): Field[FROM, Option[Iterable[Int]], _] =
      PGIntArrayOptional(extractor)

    override def smallintOptional[FROM](
        extractor: StringInterning => FROM => Option[Int]
    ): Field[FROM, Option[Int], _] =
      PGSmallintOptional(extractor)

    override def insert[FROM](tableName: String)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      PGTable.transposedInsert(tableName)(fields*)

    override def idempotentInsert[FROM](
        tableName: String,
        keyFieldIndex: Int,
        ordering: Ordering[FROM],
    )(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM] =
      PGTable.idempotentTransposedInsert(tableName, keyFieldIndex, ordering)(fields*)
  }

  val schema: Schema[DbDto] = AppendOnlySchema(PGFieldStrategy)
}
