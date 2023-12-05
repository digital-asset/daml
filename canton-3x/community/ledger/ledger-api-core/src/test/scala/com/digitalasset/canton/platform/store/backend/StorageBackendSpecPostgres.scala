// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceStorageBackend
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec

final class StorageBackendSpecPostgres
    extends AnyFlatSpec
    with StorageBackendProviderPostgres
    with StorageBackendSuite
    with StorageBackendTestsMigrationPruning
    with Inside {

  behavior of "StorageBackend (Postgres)"

  it should "find the Postgres version" in {
    val version = executeSql(PostgresDataSourceStorageBackend(loggerFactory).getPostgresVersion)

    inside(version) { case Some(versionNumbers) =>
      // Minimum Postgres version used in tests
      versionNumbers._1 should be >= 9
      versionNumbers._2 should be >= 0
    }
  }

  it should "correctly parse a Postgres version" in {
    val backend = PostgresDataSourceStorageBackend(loggerFactory)
    backend.parsePostgresVersion("1.2") shouldBe Some((1, 2))
    backend.parsePostgresVersion("1.2.3") shouldBe Some((1, 2))
    backend.parsePostgresVersion("1.2.3-alpha.4.5") shouldBe Some((1, 2))
    backend.parsePostgresVersion("10.11") shouldBe Some((10, 11))
  }

  it should "fail the compatibility check for Postgres versions lower than minimum" in {
    val version = executeSql(PostgresDataSourceStorageBackend(loggerFactory).getPostgresVersion)
    val currentlyUsedMajorVersion = inside(version) { case Some((majorVersion, _)) =>
      majorVersion
    }
    val backend =
      new PostgresDataSourceStorageBackend(
        minMajorVersionSupported = currentlyUsedMajorVersion + 1,
        loggerFactory = loggerFactory,
      )

    loggerFactory.assertThrowsAndLogs[PostgresDataSourceStorageBackend.UnsupportedPostgresVersion](
      within = executeSql(
        backend.checkCompatibility
      ),
      assertions = _.errorMessage should include(
        "Deprecated Postgres version."
      ),
    )
  }
}
