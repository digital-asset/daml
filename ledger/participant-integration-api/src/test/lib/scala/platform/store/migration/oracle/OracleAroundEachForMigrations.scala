// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.migration.oracle

import com.daml.platform.store.DbType
import com.daml.platform.store.migration.DbConnectionAndDataSourceAroundEach
import com.daml.testing.oracle.OracleAroundEach
import org.scalatest.Suite

trait OracleAroundEachForMigrations
    extends DbConnectionAndDataSourceAroundEach
    with OracleAroundEach { self: Suite =>
  override implicit def dbType: DbType = DbType.Oracle
}
