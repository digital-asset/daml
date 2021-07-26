// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.dbbackend.ContractDao
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import cats.effect.IO
import com.daml.http.util.Logging.InstanceUUID

sealed trait DbStartupResult
object DbStartupResult {
  case object GracefullyExit extends DbStartupResult
  case object Continue extends DbStartupResult

  private def fromBool(shouldGracefullyExit: Boolean): DbStartupResult =
    if (shouldGracefullyExit) GracefullyExit else Continue

  private[this] val logger = ContextualizedLogger.get(getClass)

  def fromStartupMode(dao: ContractDao, dbStartupMode: DbStartupMode)(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): IO[Option[DbStartupResult]] = {
    import dao.{logHandler, jdbcDriver}
    def none: IO[Option[DbStartupResult]] = IO.pure(None)
    def reinit(shouldGracefullyExit: Boolean): IO[Option[DbStartupResult]] = {
      logger.info("Creating DB schema...")
      dao.transact(ContractDao.initialize).map { _ =>
        logger.info("DB schema created...")
        DbStartupResult.fromBool(shouldGracefullyExit)
      }
    }.map(Some(_): Option[DbStartupResult])
    def checkVersion(errorOnMissingOrMismatch: Boolean): IO[Option[DbStartupResult]] = for {
      _ <- IO.pure(logger.info("Checking for existing schema in the DB"))
      currentSchemaVersion = jdbcDriver.queries.schemaVersion
      versionOpt <- dao.transact(jdbcDriver.queries.version())
      res <- versionOpt match {
        case None =>
          if (errorOnMissingOrMismatch) {
            logger.error(
              "No schema version found in the DB, create the schema via `start-mode=create-only` in the jdbc config"
            )
            none
          } else {
            logger.info("No schema version found in the DB, initializing schema")
            reinit(shouldGracefullyExit = false)
          }
        case Some(version) =>
          logger.info(s"DB schema version $version found")
          if (version != currentSchemaVersion) {
            val msg =
              s"Schema version mismatch, expected $currentSchemaVersion but got $version"
            if (errorOnMissingOrMismatch) {
              logger.error(
                s"$msg. Re-create the schema via `start-mode=create-only` in the jdbc config"
              )
              none
            } else {
              logger.info(msg)
              logger.info(s"Re-initializing with version $currentSchemaVersion")
              reinit(shouldGracefullyExit = false)
            }
          } else {
            logger.info("DB schema is up-to-date, continuing startup")
            IO.pure(Some(Continue))
          }
      }
    } yield res
    dbStartupMode match {
      case DbStartupMode.CreateOnly => reinit(shouldGracefullyExit = true)
      case DbStartupMode.CreateAndStart => reinit(shouldGracefullyExit = false)
      case DbStartupMode.StartOnly =>
        checkVersion(errorOnMissingOrMismatch = true)
      case DbStartupMode.CreateIfNeededAndStart =>
        checkVersion(errorOnMissingOrMismatch = false)
    }
  }
}
