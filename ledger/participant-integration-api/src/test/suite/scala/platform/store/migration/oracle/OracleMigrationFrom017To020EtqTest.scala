// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.migration.oracle

import com.daml.platform.store.migration.tests.MigrationEtqTests

class OracleMigrationFrom017To020EtqTest
    extends MigrationEtqTests
    with OracleAroundEachForMigrations {
  override def srcMigration: String = "17"
  override def dstMigration: String = "20"
}
