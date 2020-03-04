// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext

sealed trait StartupMode

object StartupMode {

  case object MigrateAndStart extends StartupMode

  case object ResetAndStart extends StartupMode

}
