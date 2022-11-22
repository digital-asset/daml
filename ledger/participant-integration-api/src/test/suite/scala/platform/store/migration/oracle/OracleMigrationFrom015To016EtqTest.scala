// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.migration.oracle

import com.daml.platform.store.migration.tests.MigrationEtqTests

class OracleMigrationFrom015To016EtqTest
    extends MigrationEtqTests
    with OracleAroundEachForMigrations {
  override def srcMigration: String = "15"
  override def dstMigration: String = "16"
}
