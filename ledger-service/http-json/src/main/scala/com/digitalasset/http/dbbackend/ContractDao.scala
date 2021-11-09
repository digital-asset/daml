// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import cats.effect._
import cats.syntax.apply._
import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.http.domain
import com.daml.http.JdbcConfig
import com.daml.http.json.JsonProtocol.LfValueDatabaseCodec
import doobie.LogHandler
import doobie.free.connection.ConnectionIO
import doobie.free.{connection => fconn}
import doobie.implicits._
import doobie.util.log
import scalaz.{NonEmptyList, OneAnd}
import scalaz.syntax.tag._
import spray.json.{JsNull, JsValue}
import cats.effect.{Blocker, ContextShift, IO}
import com.daml.scalautil.Statement.discard
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie._

import java.io.{Closeable, IOException}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.util.concurrent.Executors.newWorkStealingPool
import javax.sql.DataSource
import scala.concurrent.ExecutionContext
import scala.language.existentials
import scala.util.Try

class ContractDao private (
    ds: DataSource with Closeable,
    xa: ConnectionPool.T,
    dbAccessPool: ExecutorService,
)(implicit val jdbcDriver: SupportedJdbcDriver)
    extends Closeable {

  implicit val logHandler: log.LogHandler = Slf4jLogHandler(classOf[ContractDao])

  def transact[A](query: ConnectionIO[A]): IO[A] =
    query.transact(xa)

  def isValid(timeoutSeconds: Int): IO[Boolean] =
    fconn.isValid(timeoutSeconds).transact(xa)

  def shutdown(): Try[Unit] = {
    Try {
      dbAccessPool.shutdown()
      discard(dbAccessPool.awaitTermination(10, TimeUnit.SECONDS))
      ds.close()
    }
  }

  @throws[IOException]
  override def close(): Unit = {
    shutdown().fold(
      {
        case e: IOException => throw e
        case e => throw new IOException(e)
      },
      identity,
    )
  }
}

object ContractDao {
  private[this] val supportedJdbcDrivers = Map(
    "org.postgresql.Driver" -> SupportedJdbcDriver.Postgres,
    "oracle.jdbc.OracleDriver" -> SupportedJdbcDriver.Oracle,
  )

  lazy val supportedJdbcDriverNames = supportedJdbcDrivers.keySet filter { d =>
    scala.util.Try(Class forName d).isSuccess
  }

  def apply(cfg: JdbcConfig)(implicit
      ec: ExecutionContext
  ): ContractDao = {
    val cs: ContextShift[IO] = IO.contextShift(ec)
    implicit val sjd: SupportedJdbcDriver = supportedJdbcDrivers.getOrElse(
      cfg.driver,
      throw new IllegalArgumentException(
        s"JDBC driver ${cfg.driver} is not one of ${supportedJdbcDrivers.keySet}"
      ),
    )
    //pool for connections awaiting database access
    val es = Executors.newWorkStealingPool(cfg.poolSize)
    val (ds, conn) = ConnectionPool.connect(cfg)(ExecutionContext.fromExecutor(es), cs)
    new ContractDao(ds, conn, es)
  }

  def initialize(implicit log: LogHandler, sjd: SupportedJdbcDriver): ConnectionIO[Unit] =
    sjd.queries.dropAllTablesIfExist *> sjd.queries.initDatabase

  def lastOffset(parties: OneAnd[Set, domain.Party], templateId: domain.TemplateId.RequiredPkg)(
      implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver,
  ): ConnectionIO[Map[domain.Party, domain.Offset]] = {
    import sjd._
    for {
      tpId <- surrogateTemplateId(templateId)
      offset <- queries
        .lastOffset(domain.Party.unsubst(parties), tpId)
    } yield {
      type L[a] = Map[a, domain.Offset]
      domain.Party.subst[L, String](domain.Offset.tag.subst(offset))
    }
  }

  def updateOffset(
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      newOffset: domain.Offset,
      lastOffsets: Map[domain.Party, domain.Offset],
  )(implicit log: LogHandler, sjd: SupportedJdbcDriver): ConnectionIO[Unit] = {
    import cats.implicits._
    import sjd._
    import scalaz.OneAnd._
    import scalaz.std.set._
    import scalaz.syntax.foldable._
    val partyVector = domain.Party.unsubst(parties.toVector)
    val lastOffsetsStr: Map[String, String] =
      domain.Party.unsubst[Map[*, String], String](domain.Offset.tag.unsubst(lastOffsets))
    for {
      tpId <- queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName,
      )
      rowCount <- queries.updateOffset(partyVector, tpId, newOffset.unwrap, lastOffsetsStr)
      _ <-
        if (rowCount == partyVector.size)
          fconn.pure(())
        else
          fconn.raiseError(StaleOffsetException(parties, templateId, newOffset, lastOffsets))
    } yield ()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def selectContracts(
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      predicate: doobie.Fragment,
  )(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver,
  ): ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
    import sjd._
    for {
      tpId <- surrogateTemplateId(templateId)

      dbContracts <- queries
        .selectContracts(domain.Party.unsubst(parties), tpId, predicate)
        .to[Vector]
      domainContracts = dbContracts.map(toDomain(templateId))
    } yield domainContracts
  }

  private[http] def selectContractsMultiTemplate[Pos](
      parties: OneAnd[Set, domain.Party],
      predicates: Seq[(domain.TemplateId.RequiredPkg, doobie.Fragment)],
      trackMatchIndices: MatchedQueryMarker[Pos],
  )(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver,
  ): ConnectionIO[Vector[(domain.ActiveContract[JsValue], Pos)]] = {
    import sjd.{queries => _, _}, cats.syntax.traverse._, cats.instances.vector._
    predicates.zipWithIndex.toVector
      .traverse { case ((tid, pred), ix) =>
        surrogateTemplateId(tid) map (stid => (ix, stid, tid, pred))
      }
      .flatMap { stIdSeq =>
        val queries = stIdSeq map { case (_, stid, _, pred) => (stid, pred) }

        trackMatchIndices match {
          case MatchedQueryMarker.ByNelInt =>
            for {
              dbContracts <- sjd.queries
                .selectContractsMultiTemplate(
                  domain.Party unsubst parties,
                  queries,
                  Queries.MatchedQueryMarker.ByInt,
                )
                .toVector
                .traverse(_.to[Vector])
              tidLookup = stIdSeq.view.map { case (ix, _, tid, _) => ix -> tid }.toMap
            } yield dbContracts match {
              case Seq() => Vector.empty
              case Seq(alreadyUnique) =>
                alreadyUnique map { dbc =>
                  (toDomain(tidLookup(dbc.templateId))(dbc), NonEmptyList(dbc.templateId))
                }
              case potentialMultiMatches =>
                potentialMultiMatches.view.flatten
                  .groupBy(_.contractId)
                  .valuesIterator
                  .map { dbcs =>
                    val dbc +: dups = dbcs.toSeq // always non-empty due to groupBy
                    (
                      toDomain(tidLookup(dbc.templateId))(dbc),
                      NonEmptyList.nels(dbc, dups: _*).map(_.templateId),
                    )
                  }
                  .toVector
            }

          case MatchedQueryMarker.Unused =>
            for {
              dbContracts <- sjd.queries
                .selectContractsMultiTemplate(
                  domain.Party unsubst parties,
                  queries,
                  Queries.MatchedQueryMarker.Unused,
                )
                .to[Vector]
              tidLookup = stIdSeq.view.map { case (_, stid, tid, _) => (stid, tid) }.toMap
            } yield dbContracts map { dbc => (toDomain(tidLookup(dbc.templateId))(dbc), ()) }
        }
      }
  }

  private[http] sealed abstract class MatchedQueryMarker[+Positive]
  private[http] object MatchedQueryMarker {
    case object ByNelInt extends MatchedQueryMarker[NonEmptyList[Int]]
    case object Unused extends MatchedQueryMarker[Unit]
  }

  private[http] def fetchById(
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      contractId: domain.ContractId,
  )(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver,
  ): ConnectionIO[Option[domain.ActiveContract[JsValue]]] = {
    import sjd._
    for {
      tpId <- surrogateTemplateId(templateId)
      dbContracts <- queries.fetchById(
        domain.Party unsubst parties,
        tpId,
        domain.ContractId unwrap contractId,
      )
    } yield dbContracts.map(toDomain(templateId))
  }

  private[http] def fetchByKey(
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      key: JsValue,
  )(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver,
  ): ConnectionIO[Option[domain.ActiveContract[JsValue]]] = {
    import sjd._
    for {
      tpId <- surrogateTemplateId(templateId)
      dbContracts <- queries.fetchByKey(domain.Party unsubst parties, tpId, key)
    } yield dbContracts.map(toDomain(templateId))
  }

  private[this] def surrogateTemplateId(templateId: domain.TemplateId.RequiredPkg)(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver,
  ) =
    sjd.queries.surrogateTemplateId(
      templateId.packageId,
      templateId.moduleName,
      templateId.entityName,
    )

  private def toDomain(templateId: domain.TemplateId.RequiredPkg)(
      a: Queries.DBContract[_, JsValue, JsValue, Vector[String]]
  ): domain.ActiveContract[JsValue] =
    domain.ActiveContract(
      contractId = domain.ContractId(a.contractId),
      templateId = templateId,
      key = decodeOption(a.key),
      payload = LfValueDatabaseCodec.asLfValueCodec(a.payload),
      signatories = domain.Party.subst(a.signatories),
      observers = domain.Party.subst(a.observers),
      agreementText = a.agreementText,
    )

  private def decodeOption(a: JsValue): Option[JsValue] = a match {
    case JsNull => None
    case _ => Some(LfValueDatabaseCodec.asLfValueCodec(a))
  }

  final case class StaleOffsetException(
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      newOffset: domain.Offset,
      lastOffset: Map[domain.Party, domain.Offset],
  ) extends java.sql.SQLException(
        s"parties: $parties, templateId: $templateId, newOffset: $newOffset, lastOffset: $lastOffset",
        StaleOffsetException.SqlState,
      )

  object StaleOffsetException {
    val SqlState = "STALE_OFFSET_EXCEPTION"
  }
}

object ConnectionPool {

  type PoolSize = Int
  object PoolSize {
    val Integration = 2
    val Production = 8
  }

  type T = Transactor.Aux[IO, _ <: DataSource with Closeable]

  def connect(c: JdbcConfig)(implicit
      ec: ExecutionContext,
      cs: ContextShift[IO],
  ): (DataSource with Closeable, T) = {
    val ds = dataSource(c)
    (
      ds,
      Transactor
        .fromDataSource[IO](
          ds,
          connectEC = ec,
          blocker = Blocker liftExecutorService newWorkStealingPool(c.poolSize),
        )(IO.ioConcurrentEffect(cs), cs),
    )
  }

  private[this] def dataSource(jc: JdbcConfig) = {
    import jc._
    val c = new HikariConfig
    c.setJdbcUrl(url)
    c.setUsername(user)
    c.setPassword(password)
    c.setMinimumIdle(jc.minIdle)
    c.setConnectionTimeout(jc.connectionTimeout)
    c.setMaximumPoolSize(poolSize)
    c.setIdleTimeout(jc.idleTimeout)
    new HikariDataSource(c)
  }
}
