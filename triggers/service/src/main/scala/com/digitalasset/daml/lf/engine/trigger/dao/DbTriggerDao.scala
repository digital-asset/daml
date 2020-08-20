// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.dao

import java.util.UUID

import cats.effect.{ContextShift, IO}
import cats.syntax.apply._
import cats.syntax.functor._
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.lf.engine.trigger.{EncryptedToken, JdbcConfig, RunningTrigger, UserCredentials}
import com.typesafe.scalalogging.StrictLogging
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.log
import doobie.{Fragment, Transactor}

import java.io.{Closeable, IOException}
import javax.sql.DataSource

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}
import scala.language.existentials

object Connection {

  private[dao] type T = Transactor.Aux[IO, _ <: DataSource with Closeable]

  private[dao] def connect(c: JdbcConfig, poolSize: PoolSize)(
      implicit ec: ExecutionContext,
      cs: ContextShift[IO]): (DataSource with Closeable, T) = {
    val ds = dataSource(c, poolSize)
    (
      ds,
      Transactor
        .fromDataSource[IO](ds, connectEC = ec, transactEC = ec)(IO.ioConcurrentEffect(cs), cs))
  }

  type PoolSize = Int
  object PoolSize {
    val IntegrationTest = 2
    val Production = 8
  }

  private[this] def dataSource(jc: JdbcConfig, poolSize: PoolSize) = {
    import jc._
    val c = new HikariConfig
    c.setJdbcUrl(url)
    c.setUsername(user)
    c.setPassword(password)
    c.setMaximumPoolSize(poolSize)
    c.setIdleTimeout(10000) // ms, minimum according to log, defaults to 600s
    new HikariDataSource(c)
  }
}

final class DbTriggerDao private (dataSource: DataSource with Closeable, xa: Connection.T)
    extends RunningTriggerDao {

  private implicit val logHandler: log.LogHandler = log.LogHandler.jdkLogHandler

  private[this] val flywayMigrations = new DbFlywayMigrations(dataSource)

  private def createTables: ConnectionIO[Unit] =
    flywayMigrations.migrate()

  // NOTE(RJR) Interpolation in `sql` literals:
  // Doobie provides a `Put` typeclass that allows us to interpolate values of various types in our
  // SQL query strings. This includes basic types like `String` and `UUID` as well as unary case
  // classes wrapping these types (e.g. `EncryptedToken`). Doobie also does some formatting of these
  // values, e.g. single quotes around `String` and `UUID` values. This is NOT the case if you use
  // `Fragment.const` which will try to use a raw string as a SQL query.

  private def insertRunningTrigger(t: RunningTrigger): ConnectionIO[Unit] = {
    val partyToken: EncryptedToken = t.credentials.token
    val fullTriggerName: String = t.triggerName.toString
    val insert: Fragment = sql"""
        insert into running_triggers values (${t.triggerInstance}, $partyToken, $fullTriggerName)
      """
    insert.update.run.void
  }

  // trigger_instance is the primary key on running_triggers so this deletes
  // at most one row. Return whether or not it deleted.
  private def deleteRunningTrigger(triggerInstance: UUID): ConnectionIO[Boolean] = {
    val delete = sql"delete from running_triggers where trigger_instance = $triggerInstance"
    delete.update.run.map(_ == 1)
  }

  private def selectRunningTriggers(partyToken: EncryptedToken): ConnectionIO[Vector[UUID]] = {
    val select: Fragment = sql"""
        select trigger_instance from running_triggers
        where party_token = $partyToken
      """
    // We do not use an `order by` clause because we sort the UUIDs afterwards using Scala's
    // comparison of UUIDs (which is different to Postgres).
    select.query[UUID].to[Vector]
  }

  // Insert a package to the `dalfs` table. Do nothing if the package already exists.
  // We specify this in the `insert` since `packageId` is the primary key on the table.
  private def insertPackage(
      packageId: PackageId,
      pkg: DamlLf.ArchivePayload): ConnectionIO[Unit] = {
    val insert: Fragment = sql"""
      insert into dalfs values (${packageId.toString}, ${pkg.toByteArray}) on conflict do nothing
    """
    insert.update.run.void
  }

  private def selectPackages: ConnectionIO[List[(String, Array[Byte])]] = {
    val select: Fragment = sql"select * from dalfs order by package_id"
    select.query[(String, Array[Byte])].to[List]
  }

  private def parsePackage(
      pkgIdString: String,
      pkgPayload: Array[Byte]): Either[String, (PackageId, DamlLf.ArchivePayload)] =
    for {
      pkgId <- PackageId.fromString(pkgIdString)
      payload <- Try(DamlLf.ArchivePayload.parseFrom(pkgPayload)) match {
        case Failure(err) => Left(s"Failed to parse package with id $pkgId.\n" ++ err.toString)
        case Success(pkg) => Right(pkg)
      }
    } yield (pkgId, payload)

  private def selectAllTriggers: ConnectionIO[Vector[(UUID, String, String)]] = {
    val select: Fragment = sql"select * from running_triggers order by trigger_instance"
    select.query[(UUID, String, String)].to[Vector]
  }

  private def parseRunningTrigger(
      triggerInstance: UUID,
      token: String,
      fullTriggerName: String): Either[String, RunningTrigger] = {
    val credentials = UserCredentials(EncryptedToken(token))
    Identifier.fromString(fullTriggerName).map(RunningTrigger(triggerInstance, _, credentials))
  }

  // Drop all tables and other objects associated with the database.
  // Only used between tests for now.
  private def dropTables: ConnectionIO[Unit] = {
    val dropFlywayHistory: Fragment = sql"drop table flyway_schema_history"
    val dropTriggerTable: Fragment = sql"drop table running_triggers"
    val dropDalfTable: Fragment = sql"drop table dalfs"
    (dropFlywayHistory.update.run
      *> dropTriggerTable.update.run
      *> dropDalfTable.update.run).void
  }

  private def run[T](query: ConnectionIO[T], errorContext: String = ""): Either[String, T] = {
    Try(query.transact(xa).unsafeRunSync) match {
      case Failure(err) => Left(errorContext ++ "\n" ++ err.toString)
      case Success(res) => Right(res)
    }
  }

  override def addRunningTrigger(t: RunningTrigger): Either[String, Unit] =
    run(insertRunningTrigger(t))

  override def removeRunningTrigger(triggerInstance: UUID): Either[String, Boolean] =
    run(deleteRunningTrigger(triggerInstance))

  override def listRunningTriggers(credentials: UserCredentials): Either[String, Vector[UUID]] = {
    // Note(RJR): Postgres' ordering of UUIDs is different to Scala/Java's.
    // We sort them after the query to be consistent with the ordering when not using a database.
    run(selectRunningTriggers(credentials.token)).map(_.sorted)
  }

  // Write packages to the `dalfs` table so we can recover state after a shutdown.
  override def persistPackages(
      dar: Dar[(PackageId, DamlLf.ArchivePayload)]): Either[String, Unit] = {
    import cats.implicits._ // needed for traverse
    val insertAll = dar.all.traverse_((insertPackage _).tupled)
    run(insertAll)
  }

  def readPackages: Either[String, List[(PackageId, DamlLf.ArchivePayload)]] = {
    import cats.implicits._ // needed for traverse
    run(selectPackages, "Failed to read packages from database").flatMap(
      _.traverse((parsePackage _).tupled)
    )
  }

  def readRunningTriggers: Either[String, Vector[RunningTrigger]] = {
    import cats.implicits._ // needed for traverse
    run(selectAllTriggers, "Failed to read running triggers from database").flatMap(
      _.traverse((parseRunningTrigger _).tupled)
    )
  }

  def initialize: Either[String, Unit] =
    run(createTables, "Failed to initialize database.")

  private[trigger] def destroy(): Either[String, Unit] =
    run(dropTables, "Failed to remove database objects.")

  private[trigger] def destroyPermanently(): Try[Unit] =
    Try(dataSource.close())

  @throws[IOException]
  override def close() =
    destroyPermanently() fold ({
      case e: IOException => throw e
      case e => throw new IOException(e)
    }, identity)
}

object DbTriggerDao {
  import Connection.PoolSize, PoolSize.Production

  def apply(c: JdbcConfig, poolSize: PoolSize = Production)(
      implicit ec: ExecutionContext): DbTriggerDao = {
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    val (ds, conn) = Connection.connect(c, poolSize)
    new DbTriggerDao(ds, conn)
  }
}
