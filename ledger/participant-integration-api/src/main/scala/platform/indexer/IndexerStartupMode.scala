// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import scala.concurrent.duration.{DurationInt, FiniteDuration}

sealed trait IndexerStartupMode

object IndexerStartupMode {

  val DefaultSchemaMigrationAttempts: Int = 30
  val DefaultSchemaMigrationAttemptBackoff: FiniteDuration = 1.second
  val DefaultAllowExistingSchema: Boolean = false

  /** Verify that the index database schema is up to date (failing if it is not), then start the indexer. */
  case object ValidateAndStart extends IndexerStartupMode

  /** Migrate the index database schema as necessary, then start the indexer. */
  final case class MigrateAndStart(
      allowExistingSchema: Boolean = DefaultAllowExistingSchema
  ) extends IndexerStartupMode

  /** Wait until the index database schema is up to date. Do not start the indexer. */
  final case class ValidateAndWaitOnly(
      schemaMigrationAttempts: Int = DefaultSchemaMigrationAttempts,
      schemaMigrationAttemptBackoff: FiniteDuration = DefaultSchemaMigrationAttemptBackoff,
  ) extends IndexerStartupMode

  /** If the index database is empty, migrate it to the latest schema.
    * If the index database is NOT empty, verify that its schema is up to date (failing if it is not).
    * Then, start the indexer.
    */
  case object MigrateOnEmptySchemaAndStart extends IndexerStartupMode

}
