// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import cats.effect._
import cats.syntax.apply._
import com.daml.dbutils.ConnectionPool
import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.http.domain
import com.daml.http.json.JsonProtocol.LfValueDatabaseCodec
import com.daml.lf.crypto.Hash
import com.daml.scalautil.nonempty.+-:
import doobie.LogHandler
import doobie.free.connection.ConnectionIO
import doobie.free.{connection => fconn}
import doobie.implicits._
import doobie.util.log
import org.slf4j.LoggerFactory
import scalaz.{NonEmptyList, OneAnd}
import scalaz.syntax.tag._
import spray.json.{JsNull, JsValue}

import java.io.{Closeable, IOException}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import javax.sql.DataSource
import scala.concurrent.ExecutionContext
import scala.language.existentials
import scala.util.Try

class ContractDao private (
    ds: DataSource with Closeable,
    xa: ConnectionPool.T,
    dbAccessPool: ExecutorService,
)(implicit
    val jdbcDriver: SupportedJdbcDriver.TC
) extends Closeable {

  private val logger = LoggerFactory.getLogger(classOf[ContractDao])
  implicit val logHandler: log.LogHandler = Slf4jLogHandler(logger)

  def transact[A](query: ConnectionIO[A]): IO[A] =
    query.transact(xa)

  def isValid(timeoutSeconds: Int): IO[Boolean] =
    fconn.isValid(timeoutSeconds).transact(xa)

  def shutdown(): Try[Unit] = {
    Try {
      dbAccessPool.shutdown()
      val cleanShutdown = dbAccessPool.awaitTermination(10, TimeUnit.SECONDS)
      logger.debug(s"Clean shutdown of dbAccess pool : $cleanShutdown")
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
  import ConnectionPool.PoolSize
  private[this] val supportedJdbcDrivers = Map[String, SupportedJdbcDriver.Available](
    "org.postgresql.Driver" -> SupportedJdbcDriver.Postgres,
    "oracle.jdbc.OracleDriver" -> SupportedJdbcDriver.Oracle,
  )

  def supportedJdbcDriverNames(available: Set[String]): Set[String] =
    supportedJdbcDrivers.keySet intersect available

  def apply(cfg: JdbcConfig, poolSize: PoolSize = PoolSize.Production)(implicit
      ec: ExecutionContext
  ): ContractDao = {
    val cs: ContextShift[IO] = IO.contextShift(ec)
    val setup = for {
      sjda <- supportedJdbcDrivers
        .get(cfg.baseConfig.driver)
        .toRight(
          s"JDBC driver ${cfg.baseConfig.driver} is not one of ${supportedJdbcDrivers.keySet}"
        )
      sjdc <- configureJdbc(cfg, sjda)
    } yield {
      implicit val sjd: SupportedJdbcDriver.TC = sjdc
      //pool for connections awaiting database access
      val es = Executors.newWorkStealingPool(poolSize)
      val (ds, conn) =
        ConnectionPool.connect(cfg.baseConfig, poolSize)(ExecutionContext.fromExecutor(es), cs)
      new ContractDao(ds, conn, es)
    }
    setup.fold(msg => throw new IllegalArgumentException(msg), identity)
  }

  // XXX SC Ideally we would do this _while parsing the command line_, but that
  // will require moving
  private[this] def configureJdbc(cfg: JdbcConfig, driver: SupportedJdbcDriver.Available) =
    driver.configure(tablePrefix = cfg.tablePrefix, extraConf = cfg.backendSpecificConf)

  def initialize(implicit log: LogHandler, sjd: SupportedJdbcDriver.TC): ConnectionIO[Unit] =
    sjd.q.queries.dropAllTablesIfExist *> sjd.q.queries.initDatabase

  def lastOffset(parties: OneAnd[Set, domain.Party], templateId: domain.TemplateId.RequiredPkg)(
      implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver.TC,
  ): ConnectionIO[Map[domain.Party, domain.Offset]] = {
    import sjd.q.queries
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
  )(implicit log: LogHandler, sjd: SupportedJdbcDriver.TC): ConnectionIO[Unit] = {
    import cats.implicits._
    import sjd.q.queries
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
      sjd: SupportedJdbcDriver.TC,
  ): ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
    import sjd.q.queries
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
      sjd: SupportedJdbcDriver.TC,
  ): ConnectionIO[Vector[(domain.ActiveContract[JsValue], Pos)]] = {
    import sjd.q.{queries => sjdQueries}, cats.syntax.traverse._, cats.instances.vector._
    predicates.zipWithIndex.toVector
      .traverse { case ((tid, pred), ix) =>
        surrogateTemplateId(tid) map (stid => (ix, stid, tid, pred))
      }
      .flatMap { stIdSeq =>
        val queries = stIdSeq map { case (_, stid, _, pred) => (stid, pred) }

        trackMatchIndices match {
          case MatchedQueryMarker.ByNelInt =>
            for {
              dbContracts <- sjdQueries
                .selectContractsMultiTemplate(
                  domain.Party unsubst parties,
                  queries,
                  Queries.MatchedQueryMarker.ByInt,
                )
                .to[Vector]
              tidLookup = stIdSeq.view.map { case (ix, _, tid, _) => ix -> tid }.toMap
            } yield dbContracts map { dbc =>
              val htid +-: ttid = dbc.templateId.unwrap
              (toDomain(tidLookup(htid))(dbc), NonEmptyList(htid, ttid: _*))
            }

          case MatchedQueryMarker.Unused =>
            for {
              dbContracts <- sjdQueries
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
      sjd: SupportedJdbcDriver.TC,
  ): ConnectionIO[Option[domain.ActiveContract[JsValue]]] = {
    import sjd.q._
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
      key: Hash,
  )(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver.TC,
  ): ConnectionIO[Option[domain.ActiveContract[JsValue]]] = {
    import sjd.q._
    for {
      tpId <- surrogateTemplateId(templateId)
      dbContracts <- queries.fetchByKey(domain.Party unsubst parties, tpId, key)
    } yield dbContracts.map(toDomain(templateId))
  }

  private[this] def surrogateTemplateId(templateId: domain.TemplateId.RequiredPkg)(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver.TC,
  ) =
    sjd.q.queries.surrogateTemplateId(
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
