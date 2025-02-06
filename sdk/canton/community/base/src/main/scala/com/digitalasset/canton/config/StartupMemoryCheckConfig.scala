// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.StartupMemoryCheckConfig.ReportingLevel

final case class StartupMemoryCheckConfig(reportingLevel: ReportingLevel)

object StartupMemoryCheckConfig {
  sealed trait ReportingLevel
  object ReportingLevel {
    final case object Warn extends ReportingLevel

    final case object Ignore extends ReportingLevel

    final case object Crash extends ReportingLevel
  }
}
