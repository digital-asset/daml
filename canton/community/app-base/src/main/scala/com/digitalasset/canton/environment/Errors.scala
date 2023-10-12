// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import com.digitalasset.canton.resource.DbMigrations

sealed trait StartupError extends Product with Serializable {

  /** Node name */
  val name: String
  def message: String
  override def toString: String = s"${name}: $message"

}

/** The current action cannot be performed when the instance for the given name is running.
  */
final case class AlreadyRunning(name: String) extends StartupError {
  def message = s"node is already running: $name"
}

final case class FailedDatabaseMigration(name: String, cause: DbMigrations.Error)
    extends StartupError {
  def message: String = s"failed to migrate database of $name: $cause"
}

final case class FailedDatabaseVersionChecks(name: String, cause: DbMigrations.DatabaseVersionError)
    extends StartupError {
  def message: String = s"version checks failed for database of $name: $cause"
}

final case class FailedDatabaseConfigChecks(name: String, cause: DbMigrations.DatabaseConfigError)
    extends StartupError {
  def message: String = s"config checks failed for database of $name: $cause"
}

final case class FailedDatabaseRepairMigration(name: String, cause: DbMigrations.Error)
    extends StartupError {
  def message: String = s"failed to repair the database migration of $name: $cause"
}

final case class DidntUseForceOnRepairMigration(name: String) extends StartupError {
  def message: String =
    s"repair_migration` is a command that may lead to data corruption in the worst case if an " +
      s"incompatible database migration is subsequently applied. To use it you need to call `$name.db.repair_migration(force=true)`. " +
      s"See `help($name.db.repair_migration)` for more details. "
}

final case class StartFailed(name: String, message: String) extends StartupError

final case class ShutdownDuringStartup(name: String, message: String) extends StartupError

/** Trying to start the node when the database has pending migrations
  */
final case class PendingDatabaseMigration(name: String, pendingMigrationMessage: String)
    extends StartupError {
  def message = s"failed to initialize $name: $pendingMigrationMessage"
}

sealed trait ShutdownError {
  val name: String
  def message: String

  override def toString: String = s"${name}: $message"
}

/** Configuration for the given name was not found in the CantonConfig
  */
final case class ConfigurationNotFound(name: String) extends StartupError with ShutdownError {
  def message = s"configuration not found: $name"
}
