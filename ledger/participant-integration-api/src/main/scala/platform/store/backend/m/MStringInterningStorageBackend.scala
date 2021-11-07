// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.platform.store.backend.StringInterningStorageBackend

object MStringInterningStorageBackend extends StringInterningStorageBackend {
  override def loadStringInterningEntries(fromIdExclusive: Int, untilIdInclusive: Int)(
      connection: Connection
  ): Iterable[(Int, String)] = {
    MStore(connection).mData.stringInternings.view
      .dropWhile(_.internalId <= fromIdExclusive)
      .takeWhile(_.internalId <= untilIdInclusive)
      .map(dto => dto.internalId -> dto.externalString)
      .toVector
  }
}
