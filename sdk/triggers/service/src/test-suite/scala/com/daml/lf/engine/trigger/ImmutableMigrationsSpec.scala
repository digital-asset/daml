// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.flyway.AbstractImmutableMigrationsSpec

class ImmutableMigrationsSpec extends AbstractImmutableMigrationsSpec {
  protected override val migrationsResourcePath = "com/daml/lf/engine/trigger/db/migration"
  protected override val migrationsMinSize = 2
  protected override val hashMigrationsScriptPath = "trigger/service/hash-migrations.sh"
}
