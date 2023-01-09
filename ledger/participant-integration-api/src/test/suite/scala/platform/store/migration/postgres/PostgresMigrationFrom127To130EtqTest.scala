// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package platform.store.migration.postgres

import com.daml.platform.store.migration.tests.MigrationEtqTests

class PostgresMigrationFrom127To130EtqTest
    extends MigrationEtqTests
    with PostgresAroundEachForMigrations {
  override def srcMigration: String = "127"
  override def dstMigration: String = "130"
}
