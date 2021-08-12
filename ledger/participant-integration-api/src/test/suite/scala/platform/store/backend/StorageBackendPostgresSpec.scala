// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.platform.store.backend.postgresql.PostgresStorageBackend
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.Inside._

final class StorageBackendPostgresSpec
    extends AsyncFlatSpec
    with StorageBackendProviderPostgres
    with StorageBackendSuite {

  behavior of "StorageBackend (Postgres)"

  it should "find the Postgres version" in {
    for {
      version <- executeSql(PostgresStorageBackend.getPostgresVersion)
    } yield {
      inside(version) { case Some(versionNumbers) =>
        // Minimum Postgres version used in tests
        versionNumbers._1 should be >= 9
        versionNumbers._2 should be >= 0
        versionNumbers._3 should be >= 0
      }
    }
  }
}
