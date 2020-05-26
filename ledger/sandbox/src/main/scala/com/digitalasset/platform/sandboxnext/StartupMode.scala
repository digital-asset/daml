// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

sealed trait StartupMode

object StartupMode {

  case object MigrateAndStart extends StartupMode

  case object ResetAndStart extends StartupMode

}
