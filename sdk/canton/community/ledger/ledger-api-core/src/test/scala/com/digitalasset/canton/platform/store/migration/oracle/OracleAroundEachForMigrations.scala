// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.migration.oracle

import com.digitalasset.canton.platform.store.DbType
import com.digitalasset.canton.platform.store.migration.DbConnectionAndDataSourceAroundEach
import com.digitalasset.canton.platform.store.testing.oracle.OracleAroundEach
import org.scalatest.Suite

trait OracleAroundEachForMigrations
    extends DbConnectionAndDataSourceAroundEach
    with OracleAroundEach { self: Suite =>
  override implicit def dbType: DbType = DbType.Oracle
}
