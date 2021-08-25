// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

sealed trait IndexerStartupMode

object IndexerStartupMode {

  case object ValidateAndStart extends IndexerStartupMode

  case object MigrateAndStart extends IndexerStartupMode

  case object ResetAndStart extends IndexerStartupMode

  case object ValidateAndWaitOnly extends IndexerStartupMode

  case object MigrateOnEmptySchemaAndStart extends IndexerStartupMode

  case object EnforceEmptySchemaAndMigrate extends IndexerStartupMode

}
