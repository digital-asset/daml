// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import scala.concurrent.duration.{DurationInt, FiniteDuration}

sealed trait IndexerStartupMode

object IndexerStartupMode {

  val DefaultSchemaMigrationAttempts: Int = 30
  val DefaultSchemaMigrationAttemptBackoff: FiniteDuration = 1.second

  case object ValidateAndStart extends IndexerStartupMode

  case object MigrateAndStart extends IndexerStartupMode

  final case class ValidateAndWaitOnly(
      schemaMigrationAttempts: Int = DefaultSchemaMigrationAttempts,
      schemaMigrationAttemptBackoff: FiniteDuration = DefaultSchemaMigrationAttemptBackoff,
  ) extends IndexerStartupMode

  case object MigrateOnEmptySchemaAndStart extends IndexerStartupMode

}
