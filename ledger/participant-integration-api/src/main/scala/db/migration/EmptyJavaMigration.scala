// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.db.migration

import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

/** A Java migration that does nothing.
  *
  * Note: Flyway does not keep the checksum of Java migrations.
  * Flyway will not complain if you replace old Java migrations by this no-op,
  * however you MUST manually make sure that your migration process will not break by this.
  */
private[migration] class EmptyJavaMigration extends BaseJavaMigration {
  override def migrate(context: Context): Unit = ()
}
