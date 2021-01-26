// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.db.migration.postgres

import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

private[migration] final class V28__Fix_key_hashes extends BaseJavaMigration {

  override def migrate(context: Context): Unit = {
    // Content of migration moved to V32_1 (see https://github.com/digital-asset/daml/issues/6017)
    // This does not break if someone already executed this migration
    // because Flyway does not keep checksums for Java-based migrations
    // The only downside is that the operation is performed a second time
    // without a real need, but it's safe to do it
  }
}
