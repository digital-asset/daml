// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import java.time.Instant

import com.daml.platform.store.backend.DbDto
import com.daml.platform.store.backend.common.AppendOnlySchema.FieldStrategy
import com.daml.platform.store.backend.common.{AppendOnlySchema, Field, Schema, Table}

private[oracle] object OracleSchema {
  private val OracleFieldStrategy = new FieldStrategy {
    override def string[FROM, _](extractor: FROM => String): Field[FROM, String, _] =
      OracleStringField(extractor)

    override def stringOptional[FROM, _](extractor: FROM => Option[String]): Field[FROM, Option[String], _] =
      OracleStringOptional(extractor)

    override def stringArray[FROM, _](
                              extractor: FROM => Iterable[String]
                            ): Field[FROM, Iterable[String], _] =
      OracleStringArray(extractor)

    override def stringArrayOptional[FROM, _](
                                      extractor: FROM => Option[Iterable[String]]
                                    ): Field[FROM, Option[Iterable[String]], _] =
      OracleStringArrayOptional(extractor)

    override def bytea[FROM, _](extractor: FROM => Array[Byte]): Field[FROM, Array[Byte], _] =
      OracleBytea(extractor)

    override def byteaOptional[FROM, _](
                                extractor: FROM => Option[Array[Byte]]
                              ): Field[FROM, Option[Array[Byte]], _] =
      OracleByteaOptional(extractor)

    override def bigint[FROM, _](extractor: FROM => Long): Field[FROM, Long, _] =
      OracleBigint(extractor)

    override def smallintOptional[FROM, _](extractor: FROM => Option[Int]): Field[FROM, Option[Int], _] =
      OracleSmallintOptional(extractor)

    override def timestamp[FROM, _](extractor: FROM => Instant): Field[FROM, Instant, _] =
      OracleTimestamp(extractor)

    override def timestampOptional[FROM, _](
                                    extractor: FROM => Option[Instant]
                                  ): Field[FROM, Option[Instant], _] =
      OracleTimestampOptional(extractor)

    override def intOptional[FROM, _](extractor: FROM => Option[Int]): Field[FROM, Option[Int], _] =
      OracleIntOptional(extractor)

    override def boolean[FROM, _](extractor: FROM => Boolean): Field[FROM, Boolean, _] =
      OracleBooleanField(extractor)

    override def booleanOptional[FROM, _](
                                  extractor: FROM => Option[Boolean]
                                ): Field[FROM, Option[Boolean], _] =
      OracleBooleanOptional(extractor)

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
