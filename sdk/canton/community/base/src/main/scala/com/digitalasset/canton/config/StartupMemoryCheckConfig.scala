// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

sealed trait StartupMemoryCheckConfig

object StartupMemoryCheckConfig {
  final case object Warn extends StartupMemoryCheckConfig
  final case object Ignore extends StartupMemoryCheckConfig
  final case object Crash extends StartupMemoryCheckConfig
}
