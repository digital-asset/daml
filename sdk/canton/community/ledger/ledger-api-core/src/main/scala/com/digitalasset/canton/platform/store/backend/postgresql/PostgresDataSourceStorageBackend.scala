// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import anorm.SqlParser.get
import anorm.SqlStringInterpolation
import com.daml.resources.ProgramResource.StartupException
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.backend.DataSourceStorageBackend
import com.digitalasset.canton.platform.store.backend.common.{
  DataSourceStorageBackendImpl,
  InitHookDataSourceProxy,
}
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig.SynchronousCommitValue
import com.digitalasset.canton.tracing.TraceContext
import org.postgresql.ds.PGSimpleDataSource

import java.sql.Connection
import javax.sql.DataSource

final case class PostgresDataSourceConfig(
    synchronousCommit: Option[SynchronousCommitValue] = None,
    // TCP keepalive configuration for postgres. See https://www.postgresql.org/docs/13/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS for details
    tcpKeepalivesIdle: Option[Int] = Some(10), // corresponds to: tcp_keepalives_idle
    tcpKeepalivesInterval: Option[Int] = Some(1), // corresponds to: tcp_keepalives_interval
    tcpKeepalivesCount: Option[Int] = Some(5), // corresponds to: tcp_keepalives_count
)

object PostgresDataSourceConfig {
  sealed abstract class SynchronousCommitValue(val pgSqlName: String)
  object SynchronousCommitValue {
    case object On extends SynchronousCommitValue("on")
    case object Off extends SynchronousCommitValue("off")
    case object RemoteWrite extends SynchronousCommitValue("remote_write")
    case object RemoteApply extends SynchronousCommitValue("remote_apply")
    case object Local extends SynchronousCommitValue("local")
    val All: Set[SynchronousCommitValue] = Set(
      On,
      Off,
      RemoteWrite,
      RemoteApply,
      Local,
    )
  }
}

class PostgresDataSourceStorageBackend(
    minMajorVersionSupported: Int,
    val loggerFactory: NamedLoggerFactory,
) extends DataSourceStorageBackend
    with NamedLogging {

  override def createDataSource(
      dataSourceConfig: DataSourceStorageBackend.DataSourceConfig,
      loggerFactory: NamedLoggerFactory,
      connectionInitHook: Option[Connection => Unit],
  ): DataSource = {
    import DataSourceStorageBackendImpl.exe
    val pgSimpleDataSource = new PGSimpleDataSource()
    pgSimpleDataSource.setUrl(dataSourceConfig.jdbcUrl)

    val hookFunctions = List(
      dataSourceConfig.postgresConfig.synchronousCommit.toList
        .map(synchCommitValue => exe(s"SET synchronous_commit TO ${synchCommitValue.pgSqlName}")),
      dataSourceConfig.postgresConfig.tcpKeepalivesIdle.toList
        .map(i => exe(s"SET tcp_keepalives_idle TO $i")),
      dataSourceConfig.postgresConfig.tcpKeepalivesInterval.toList
        .map(i => exe(s"SET tcp_keepalives_interval TO $i")),
      dataSourceConfig.postgresConfig.tcpKeepalivesCount.toList
        .map(i => exe(s"SET tcp_keepalives_count TO $i")),
      connectionInitHook.toList,
    ).flatten
    InitHookDataSourceProxy(pgSimpleDataSource, hookFunctions, loggerFactory)
  }

  override def checkCompatibility(
      connection: Connection
  )(implicit traceContext: TraceContext): Unit = {
    getPostgresVersion(connection) match {
      case Some((major, minor)) =>
        if (major < minMajorVersionSupported) {
          val errorMessage =
            "Deprecated Postgres version. " +
              s"Found Postgres version $major.$minor, minimum required Postgres version is $minMajorVersionSupported. " +
              "This application will continue running but is at risk of data loss, as Postgres < 10 does not support crash-fault tolerant hash indices. " +
              s"Please upgrade your Postgres database to version $minMajorVersionSupported or later to fix this issue."
          logger.error(errorMessage)
          throw new PostgresDataSourceStorageBackend.UnsupportedPostgresVersion(errorMessage)
        }
      case None =>
        logger.warn(
          s"Could not determine the version of the Postgres database. Please verify that this application is compatible with this Postgres version."
        )
    }
    ()
  }

  private[backend] def getPostgresVersion(
      connection: Connection
  )(implicit traceContext: TraceContext): Option[(Int, Int)] = {
    val version = SQL"SHOW server_version".as(get[String](1).single)(connection)
    logger.debug(s"Found Postgres version $version")
    parsePostgresVersion(version)
  }

  private[backend] def parsePostgresVersion(version: String): Option[(Int, Int)] = {
    val versionPattern = """(\d+)[.](\d+).*""".r
    version match {
      case versionPattern(major, minor) => Some((major.toInt, minor.toInt))
      case _ => None
    }
  }

  override def checkDatabaseAvailable(connection: Connection): Unit =
    DataSourceStorageBackendImpl.checkDatabaseAvailable(connection)
}

object PostgresDataSourceStorageBackend {
  def apply(loggerFactory: NamedLoggerFactory): PostgresDataSourceStorageBackend =
    new PostgresDataSourceStorageBackend(minMajorVersionSupported = 10, loggerFactory)

  final class UnsupportedPostgresVersion(message: String)
      extends RuntimeException(message)
      with StartupException
}
