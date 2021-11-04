// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.platform.store.backend.postgresql.PostgresDataSourceStorageBackend
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec

final class StorageBackendPostgresSpec
    extends AsyncFlatSpec
    with StorageBackendProviderPostgres
    with StorageBackendSuite
    with StorageBackendTestsMigrationPruning
    with Inside {

  behavior of "StorageBackend (Postgres)"

  it should "find the Postgres version" in {
    for {
      version <- executeSql(PostgresDataSourceStorageBackend.getPostgresVersion)
    } yield {
      inside(version) { case Some(versionNumbers) =>
        // Minimum Postgres version used in tests
        versionNumbers._1 should be >= 9
        versionNumbers._2 should be >= 0
      }
    }
  }

  it should "correctly parse a Postgres version" in {
    PostgresDataSourceStorageBackend.parsePostgresVersion("1.2") shouldBe Some((1, 2))
    PostgresDataSourceStorageBackend.parsePostgresVersion("1.2.3") shouldBe Some((1, 2))
    PostgresDataSourceStorageBackend.parsePostgresVersion("1.2.3-alpha.4.5") shouldBe Some((1, 2))
    PostgresDataSourceStorageBackend.parsePostgresVersion("10.11") shouldBe Some((10, 11))
  }
}
