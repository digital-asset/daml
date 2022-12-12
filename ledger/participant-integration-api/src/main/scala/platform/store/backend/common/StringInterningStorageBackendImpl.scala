// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.SqlParser.{int, str}
import anorm.{RowParser, SqlStringInterpolation, ~}
import com.daml.platform.store.backend.StringInterningStorageBackend
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._

object StringInterningStorageBackendImpl extends StringInterningStorageBackend {

  private val StringInterningEntriesParser: RowParser[(Int, String)] =
    int("internal_id") ~ str("external_string") map { case internalId ~ externalString =>
      (internalId, externalString)
    }

  override def loadStringInterningEntries(fromIdExclusive: Int, untilIdInclusive: Int)(
      connection: Connection
  ): Iterable[(Int, String)] =
    SQL"""
         SELECT internal_id, external_string
         FROM string_interning
         WHERE
           internal_id > $fromIdExclusive
           AND internal_id <= $untilIdInclusive
       """.asVectorOf(StringInterningEntriesParser)(connection)
}
