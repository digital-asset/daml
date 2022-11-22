// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package platform.store.migration.postgres

import com.daml.platform.store.migration.tests.MigrationEtqTests

class PostgresMigrationFrom125To126EtqTest
    extends MigrationEtqTests
    with PostgresAroundEachForMigrations {
  override def srcMigration: String = "125"
  override def dstMigration: String = "126"
}
