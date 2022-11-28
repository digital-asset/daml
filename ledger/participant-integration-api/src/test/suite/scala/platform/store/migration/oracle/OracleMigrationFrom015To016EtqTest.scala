// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.migration.oracle

import com.daml.platform.store.migration.tests.MigrationEtqTests
import org.scalatest.Ignore

// TODO etq: Enable when data migration is added
@Ignore
class OracleMigrationFrom015To016EtqTest
    extends MigrationEtqTests
    with OracleAroundEachForMigrations {
  override def srcMigration: String = "16"
  override def dstMigration: String = "17"
}
