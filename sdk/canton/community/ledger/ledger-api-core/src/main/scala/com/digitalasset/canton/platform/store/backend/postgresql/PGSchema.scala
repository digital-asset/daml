// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    ): Field[FROM, Iterable[String], ?] =
      PGStringArray(extractor)

    override def smallint[FROM](
        extractor: StringInterning => FROM => Int
    ): Field[FROM, Int, ?] =
      PGSmallint(extractor)

    override def smallintOptional[FROM](
        extractor: StringInterning => FROM => Option[Int]
    ): Field[FROM, Option[Int], ?] =
      PGSmallintOptional(extractor)

    override def insert[FROM](tableName: String)(
        fields: (String, Field[FROM, ?, ?])*
    ): Table[FROM] =
      PGTable.transposedInsert(tableName)(fields*)
  }

  val schema: Schema[DbDto] = AppendOnlySchema(PGFieldStrategy)
}
