// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.dbbackend.ContractDao
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import cats.effect.IO
import com.daml.http.util.Logging.InstanceUUID

sealed trait SchemaHandlingResult
object SchemaHandlingResult {
  case object Terminate extends SchemaHandlingResult
  case object Continue extends SchemaHandlingResult

  def fromBool(shouldTerminate: Boolean): SchemaHandlingResult =
    if (shouldTerminate) Terminate else Continue

  private[this] val logger = ContextualizedLogger.get(getClass)

  def fromSchemaHandling(dao: ContractDao, schemaHandling: SchemaHandling)(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): IO[SchemaHandlingResult] = {
    import dao.{logHandler, jdbcDriver}
    def terminate: IO[SchemaHandlingResult] = IO.pure(Terminate)
    def reinit(shouldTerminate: Boolean): IO[SchemaHandlingResult] = {
      logger.info("Creating DB schema...")
      dao.transact(ContractDao.initialize).map { _ =>
        logger.info("DB schema created...")
        SchemaHandlingResult.fromBool(shouldTerminate)
      }
    }
    def checkVersion(shouldTerminate: Boolean): IO[SchemaHandlingResult] = {
      logger.info("Checking for existing schema")
      dao.transact(jdbcDriver.queries.version()).flatMap {
        case None =>
          logger.info("No schema version found")
          if (shouldTerminate) terminate
          else reinit(shouldTerminate = false)
        case Some(version) =>
          logger.info(s"DB schema version $version found")
          if (version < jdbcDriver.queries.schemaVersion) {
            logger.info("DB schema version is not up to date")
            if (shouldTerminate) terminate
            else {
              logger.info(s"Re-initializing with version ${jdbcDriver.queries.schemaVersion}")
              reinit(shouldTerminate = false)
            }
          } else {
            logger.info("DB schema is up-to-date, continuing startup")
            IO.pure(Continue)
          }
      }
    }
    schemaHandling match {
      case SchemaHandling.ForceCreateAndTerminate => reinit(shouldTerminate = true)
      case SchemaHandling.ForceCreateAndContinue => reinit(shouldTerminate = false)
      case SchemaHandling.CheckAndTerminateIfWrong =>
        checkVersion(shouldTerminate = true)
      case SchemaHandling.CreateOrUpdateAndContinue =>
        checkVersion(shouldTerminate = false)
    }
  }
}
