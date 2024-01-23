// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.environment.{CantonNode, CantonNodeBootstrap, Nodes}
import com.digitalasset.canton.tracing.TraceContext

/** Aliases to manage a sequence of instances in a REPL environment
  */
trait LocalInstancesExtensions[LocalInstanceRef <: LocalInstanceReferenceCommon] extends Helpful {

  import ConsoleCommandResult.runAll

  def instances: Seq[LocalInstanceRef]

  @Help.Summary("Database management related operations")
  @Help.Group("Database")
  object db extends Helpful {

    @Help.Summary("Migrate all databases")
    def migrate()(implicit consoleEnvironment: ConsoleEnvironment): Unit = {
      val _ = runAll(instances.sorted(consoleEnvironment.startupOrdering)) {
        _.migrateDbCommand()
      }
    }

    @Help.Summary("Only use when advised - repair the database migration of all nodes")
    @Help.Description(
      """In some rare cases, we change already applied database migration files in a new release and the repair
        |command resets the checksums we use to ensure that in general already applied migration files have not been changed.
        |You should only use `db.repair_migration` when advised and otherwise use it at your own risk - in the worst case running
        |it may lead to data corruption when an incompatible database migration (one that should be rejected because
        |the already applied database migration files have changed) is subsequently falsely applied.
        |"""
    )
    def repair_migration(
        force: Boolean = false
    )(implicit consoleEnvironment: ConsoleEnvironment): Unit = {
      val _ = runAll(instances.sorted(consoleEnvironment.startupOrdering)) {
        _.repairMigrationCommand(force)
      }
    }

  }

  private def runOnAllInstances[T](
      cmd: Seq[(String, Nodes[CantonNode, CantonNodeBootstrap[CantonNode]])] => Either[T, Unit]
  )(implicit consoleEnvironment: ConsoleEnvironment): Unit =
    consoleEnvironment.runE(cmd(instances.map(x => (x.name, x.nodes))))

  @Help.Summary("Start all")
  def start()(implicit consoleEnvironment: ConsoleEnvironment): Unit =
    TraceContext.withNewTraceContext { implicit traceContext =>
      runOnAllInstances(consoleEnvironment.environment.startNodes(_))
    }

  @Help.Summary("Stop all")
  def stop()(implicit consoleEnvironment: ConsoleEnvironment): Unit =
    TraceContext.withNewTraceContext { implicit traceContext =>
      runOnAllInstances(consoleEnvironment.environment.stopNodes(_))
    }

}

object LocalInstancesExtensions {
  class Impl[LocalInstanceRef <: LocalInstanceReferenceCommon](val instances: Seq[LocalInstanceRef])
      extends LocalInstancesExtensions[LocalInstanceRef] {}
}
