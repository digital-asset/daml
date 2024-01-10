// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.migration.postgres

import com.digitalasset.canton.platform.store.DbType
import com.digitalasset.canton.platform.store.migration.DbConnectionAndDataSourceAroundEach
import com.digitalasset.canton.platform.store.testing.postgresql.PostgresAroundEach
import org.scalatest.Suite

/** Creates a fresh data source and connection for each test case
  */
trait PostgresAroundEachForMigrations
    extends DbConnectionAndDataSourceAroundEach
    with PostgresAroundEach {
  self: Suite =>
  override implicit def dbType: DbType = DbType.Postgres
}
