// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlagFilter,
  Helpful,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}

trait ConsoleCommandGroup extends Helpful with FeatureFlagFilter with NamedLogging {
  protected def runner: AdminCommandRunner
  protected def consoleEnvironment: ConsoleEnvironment
  private[commands] def myLoggerFactory: NamedLoggerFactory = loggerFactory
}

object ConsoleCommandGroup {
  class Impl(parent: ConsoleCommandGroup) extends ConsoleCommandGroup {
    override protected def consoleEnvironment: ConsoleEnvironment = parent.consoleEnvironment
    override protected def runner: AdminCommandRunner = parent.runner
    override protected def loggerFactory: NamedLoggerFactory = parent.myLoggerFactory
  }
}
