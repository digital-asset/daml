// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.dbbackend.ContractDao
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import cats.effect.IO
import com.daml.http.util.Logging.InstanceUUID
import doobie.LogHandler
import com.daml.http.dbbackend.DbStartupMode

object DbStartupOps {

  private[this] val logger = ContextualizedLogger.get(getClass)

  sealed trait DbVersionState
  object DbVersionState {
    final case object Missing extends DbVersionState
    final case object UpToDate extends DbVersionState
    final case class Mismatch(expected: Int, actual: Int) extends DbVersionState
  }
  import DbStartupMode._
  import DbVersionState._

  def getDbVersionState(implicit
      dao: ContractDao,
      lc: LoggingContextOf[InstanceUUID],
      log: LogHandler,
  ): IO[Either[Throwable, DbVersionState]] = {
    import dao.jdbcDriver, jdbcDriver.q.queries, scalaz.Scalaz._
    for {
      _ <- IO(logger.info("Checking for existing schema in the DB"))
      currentSchemaVersion = queries.schemaVersion
      getVersionResult <- dao.transact(queries.version()).attempt
    } yield getVersionResult
      .bimap(
        { err =>
          logger.error("Failed to query DB schema version", err)
          err
        },
        {
          case None => Missing
          case Some(version: Int) =>
            if (version != currentSchemaVersion) Mismatch(currentSchemaVersion, version)
            else UpToDate
        },
      )
  }

  def initialize(implicit dao: ContractDao, lc: LoggingContextOf[InstanceUUID]): IO[Boolean] = {
    import dao.{logHandler, jdbcDriver}
    for {
      _ <- IO(logger.info(s"Creating DB schema, version ${jdbcDriver.q.queries.schemaVersion}"))
      res <- dao.transact(ContractDao.initialize).attempt
      _ = res.fold(
        e => logger.error("Failed to initialize DB", e),
        _ => logger.info("DB schema created..."),
      )
    } yield res.isRight
  }

  def fromStartupMode(dao: ContractDao, dbStartupMode: DbStartupMode)(implicit
      _dao: ContractDao = dao,
      lc: LoggingContextOf[InstanceUUID],
  ): IO[Boolean] = {
    import dao.logHandler
    def checkDbVersionStateAnd(createOrUpdate: Boolean): IO[Boolean] = {
      val ioFalse = IO.pure(false)
      val dbVersionState = getDbVersionState
      val hintMsg = "Re-create the schema via `start-mode=create-only` in the jdbc config"
      dbVersionState.flatMap(_.map {
        case Missing =>
          val msg = "No schema version found in the DB"
          if (createOrUpdate) {
            logger.info(s"$msg")
            initialize
          } else {
            logger.error(s"$msg. $hintMsg")
            ioFalse
          }
        case Mismatch(expected, actual) =>
          val msg = s"Schema version mismatch, expected $expected but got $actual"
          if (createOrUpdate) {
            logger.info(msg)
            initialize
          } else {
            logger.error(
              s"$msg. $hintMsg"
            )
            ioFalse
          }
        case UpToDate =>
          logger.info("DB schema is up-to-date")
          IO.pure(true)
      }.getOrElse(ioFalse))
    }
    dbStartupMode match {
      case CreateOnly | CreateAndStart => initialize
      case StartOnly => checkDbVersionStateAnd(createOrUpdate = false)
      case CreateIfNeededAndStart => checkDbVersionStateAnd(createOrUpdate = true)
    }
  }

  def shouldStart(dbStartup: DbStartupMode): Boolean = dbStartup match {
    case CreateIfNeededAndStart | CreateAndStart | StartOnly => true
    case CreateOnly => false
  }
}
