// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.flyway.AbstractImmutableMigrationsSpec

class ImmutableMigrationsSpec extends AbstractImmutableMigrationsSpec {
  protected override val migrationsResourcePath = "com/daml/ledger/on/sql/migrations"
  protected override val migrationsMinSize = 3
  protected override val hashMigrationsScriptPath = "ledger/ledger-on-sql/hash-migrations.sh"
}
