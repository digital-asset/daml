// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.dao

import java.util.UUID
import java.util.concurrent.Executors.newWorkStealingPool

import cats.effect.{Blocker, ContextShift, IO}
import cats.syntax.apply._
import cats.syntax.functor._
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.lf.archive.{Dar, Reader}
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.lf.engine.trigger.{JdbcConfig, RunningTrigger}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.{Get, log}
import doobie.{Fragment, Put, Transactor}
import scalaz.Tag
import java.io.{Closeable, IOException}

import com.daml.lf.engine.trigger.Tagged.AccessToken
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
        .fromDataSource[IO](
          ds,
          connectEC = ec,
          blocker = Blocker liftExecutorService newWorkStealingPool(poolSize))(
          IO.ioConcurrentEffect(cs),
          cs))
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

  implicit val partyPut: Put[Party] = Tag.subst(implicitly[Put[String]])

  implicit val partyGet: Get[Party] = Tag.subst(implicitly[Get[String]])

  implicit val appIdPut: Put[ApplicationId] = Tag.subst(implicitly[Put[String]])

  implicit val appIdGet: Get[ApplicationId] = Tag.subst(implicitly[Get[String]])

  implicit val accessTokenPut: Put[AccessToken] = Tag.subst(implicitly[Put[String]])

  implicit val accessTokenGet: Get[AccessToken] = Tag.subst(implicitly[Get[String]])

  implicit val identifierPut: Put[Identifier] = implicitly[Put[String]].contramap(_.toString)

  implicit val identifierGet: Get[Identifier] =
    implicitly[Get[String]].map(Identifier.assertFromString(_))

  private implicit val logHandler: log.LogHandler = log.LogHandler.jdkLogHandler

  private[this] val flywayMigrations = new DbFlywayMigrations(dataSource)

  private def createTables: ConnectionIO[Unit] =
    flywayMigrations.migrate()

  // NOTE(RJR) Interpolation in `sql` literals:
  // Doobie provides a `Put` typeclass that allows us to interpolate values of various types in our
  // SQL query strings. This includes basic types like `String` and `UUID` as well as unary case
  // classes wrapping these types. Doobie also does some formatting of these
  // values, e.g. single quotes around `String` and `UUID` values. This is NOT the case if you use
  // `Fragment.const` which will try to use a raw string as a SQL query.

  private def insertRunningTrigger(t: RunningTrigger): ConnectionIO[Unit] = {
    val insert: Fragment = sql"""
        insert into running_triggers
          (trigger_instance, trigger_party, full_trigger_name, access_token, application_id)
        values
          (${t.triggerInstance}, ${t.triggerParty}, ${t.triggerName}, ${t.triggerToken}, ${t.triggerApplicationId})
      """
    insert.update.run.void
  }

  private def queryRunningTrigger(triggerInstance: UUID): ConnectionIO[Option[RunningTrigger]] = {
    val select: Fragment = sql"""
        select trigger_instance, full_trigger_name, trigger_party, application_id, access_token from running_triggers
        where trigger_instance = $triggerInstance
      """
    select
      .query[(UUID, Identifier, Party, ApplicationId, Option[AccessToken])]
      .map(RunningTrigger.tupled)
      .option
  }

  // trigger_instance is the primary key on running_triggers so this deletes
  // at most one row. Return whether or not it deleted.
  private def deleteRunningTrigger(triggerInstance: UUID): ConnectionIO[Boolean] = {
    val delete = sql"delete from running_triggers where trigger_instance = $triggerInstance"
    delete.update.run.map(_ == 1)
  }

  private def selectRunningTriggers(party: Party): ConnectionIO[Vector[UUID]] = {
    val select: Fragment = sql"""
        select trigger_instance from running_triggers
        where trigger_party = $party
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
      cos = Reader.damlLfCodedInputStreamFromBytes(pkgPayload)
      payload <- Try(DamlLf.ArchivePayload.parseFrom(cos)) match {
        case Failure(err) => Left(s"Failed to parse package with id $pkgId.\n" ++ err.toString)
        case Success(pkg) => Right(pkg)
      }
    } yield (pkgId, payload)

  private def selectAllTriggers: ConnectionIO[Vector[RunningTrigger]] = {
    val select: Fragment = sql"""
      select trigger_instance, full_trigger_name, trigger_party, application_id, access_token from running_triggers order by trigger_instance
    """
    select
      .query[(UUID, Identifier, Party, ApplicationId, Option[AccessToken])]
      .map(RunningTrigger.tupled)
      .to[Vector]
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

  override def getRunningTrigger(triggerInstance: UUID): Either[String, Option[RunningTrigger]] =
    run(queryRunningTrigger(triggerInstance))

  override def removeRunningTrigger(triggerInstance: UUID): Either[String, Boolean] =
    run(deleteRunningTrigger(triggerInstance))

  override def listRunningTriggers(party: Party): Either[String, Vector[UUID]] = {
    // Note(RJR): Postgres' ordering of UUIDs is different to Scala/Java's.
    // We sort them after the query to be consistent with the ordering when not using a database.
    run(selectRunningTriggers(party)).map(_.sorted)
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

  def readRunningTriggers: Either[String, Vector[RunningTrigger]] =
    run(selectAllTriggers, "Failed to read running triggers from database")

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
