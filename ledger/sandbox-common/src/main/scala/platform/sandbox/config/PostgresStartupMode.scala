// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.config

sealed trait PostgresStartupMode

object PostgresStartupMode {

  case object MigrateOnly extends PostgresStartupMode

  case object MigrateAndStart extends PostgresStartupMode

  case object ValidateAndStart extends PostgresStartupMode

}
