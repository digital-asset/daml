// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

sealed trait IndexerStartupMode

object IndexerStartupMode {

  case object ValidateAndStart extends IndexerStartupMode

  case object MigrateAndStart extends IndexerStartupMode

  case object MigrateOnly extends IndexerStartupMode

}
