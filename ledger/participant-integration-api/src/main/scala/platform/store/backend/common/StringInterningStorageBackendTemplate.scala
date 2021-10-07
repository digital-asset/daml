// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.SqlParser.{int, str}
import anorm.{SqlStringInterpolation, ~}
import com.daml.platform.store.backend.StringInterningStorageBackend
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf

trait StringInterningStorageBackendTemplate extends StringInterningStorageBackend {

  override def loadStringInterningEntries(fromIdExclusive: Int, untilIdInclusive: Int)(
      connection: Connection
  ): Iterable[(Int, String)] =
    SQL"""
         SELECT id, s
         FROM string_interning
         WHERE
           id > $fromIdExclusive
           AND id <= $untilIdInclusive
       """.asVectorOf((int("id") ~ str("s")).map { case id ~ s =>
      (id, s)
    })(connection)
}
