// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import cats.effect._
import cats.syntax.apply._
import com.daml.dbutils.ConnectionPool
import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.http.dbbackend.Queries.SurrogateTpId
import com.daml.http.domain
import com.daml.http.json.JsonProtocol.LfValueDatabaseCodec
import com.daml.http.util.Logging.InstanceUUID
import com.daml.lf.crypto.Hash
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Metrics
import com.daml.nonempty.{+-:, NonEmpty, NonEmptyF}
import domain.Offset.`Offset ordering`
import doobie.LogHandler
import doobie.free.connection.ConnectionIO
import doobie.free.{connection => fconn}
import doobie.implicits._
import doobie.util.log
import org.slf4j.LoggerFactory
import scalaz.{Equal, NonEmptyList, Order, Semigroup}
import scalaz.std.set._
import scalaz.std.vector._
import scalaz.syntax.tag._
import scalaz.syntax.order._
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
  import SurrogateTemplateIdCache.MaxEntries
  private[this] val supportedJdbcDrivers = Map[String, SupportedJdbcDriver.Available](
    "org.postgresql.Driver" -> SupportedJdbcDriver.Postgres,
    "oracle.jdbc.OracleDriver" -> SupportedJdbcDriver.Oracle,
  )

  def supportedJdbcDriverNames(available: Set[String]): Set[String] =
    supportedJdbcDrivers.keySet intersect available

  private[this] def queriesPartySet(dps: domain.PartySet): Queries.PartySet = {
    type NES[A] = NonEmpty[Set[A]]
    domain.Party.unsubst[NES, String](dps)
  }

  def apply(
      cfg: JdbcConfig,
      tpIdCacheMaxEntries: Option[Long] = None,
  )(implicit
      ec: ExecutionContext,
      metrics: Metrics,
  ): ContractDao = {
    val cs: ContextShift[IO] = IO.contextShift(ec)
    val setup = for {
      sjda <- supportedJdbcDrivers
        .get(cfg.baseConfig.driver)
        .toRight(
          s"JDBC driver ${cfg.baseConfig.driver} is not one of ${supportedJdbcDrivers.keySet}"
        )
      sjdc <- configureJdbc(cfg, sjda, tpIdCacheMaxEntries.getOrElse(MaxEntries))
    } yield {
      implicit val sjd: SupportedJdbcDriver.TC = sjdc
      // pool for connections awaiting database access
      val es = Executors.newWorkStealingPool(cfg.baseConfig.poolSize)
      val (ds, conn) =
        ConnectionPool.connect(cfg.baseConfig)(ExecutionContext.fromExecutor(es), cs)
      new ContractDao(ds, conn, es)
    }
    setup.fold(msg => throw new IllegalArgumentException(msg), identity)
  }

  // XXX SC Ideally we would do this _while parsing the command line_, but that
  // will require moving selection and setup of a driver into a hook that the
  // cmdline parser can use while constructing JdbcConfig.  That's a good idea
  // anyway, and is significantly easier with the `Conf` separation
  private[this] def configureJdbc(
      cfg: JdbcConfig,
      driver: SupportedJdbcDriver.Available,
      tpIdCacheMaxEntries: Long,
  )(implicit
      metrics: Metrics
  ) =
    driver.configure(
      tablePrefix = cfg.baseConfig.tablePrefix,
      extraConf = cfg.backendSpecificConf,
      tpIdCacheMaxEntries,
    )

  def initialize(implicit log: LogHandler, sjd: SupportedJdbcDriver.TC): ConnectionIO[Unit] =
    sjd.q.queries.dropAllTablesIfExist *> sjd.q.queries.initDatabase

  def lastOffset(parties: domain.PartySet, templateId: domain.TemplateId.RequiredPkg)(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver.TC,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[Map[domain.Party, domain.Offset]] = {
    import sjd.q.queries
    for {
      tpId <- surrogateTemplateId(templateId)
      offset <- queries
        .lastOffset(queriesPartySet(parties), tpId)
    } yield {
      type L[a] = Map[a, domain.Offset]
      domain.Party.subst[L, String](domain.Offset.tag.subst(offset))
    }
  }

  /** A "lagging offset" is a template-ID/party pair whose stored offset may not reflect
    * the actual contract table state at query time.  Examples of this are described in
    * <https://github.com/digital-asset/daml/issues/10334> .
    *
    * It is perfectly fine for an offset to be returned but the set of lagging template IDs
    * be empty; this means they appear to be consistent but not at `expectedOffset`; since
    * that means the state at query time is indeterminate, the query must be rerun anyway,
    * but no further DB updates are needed.
    *
    * @param parties The party-set that must not be lagging.  We aren't concerned with
    *          whether ''other'' parties are lagging, in fact we can't correct if they are,
    *          but if another party got ahead, that means one of our parties lags.
    * @param expectedOffset `fetchAndPersist` should have synchronized every template-ID/party
    *          pair to the same offset, this one.
    * @param templateIds The template IDs we're checking.
    * @return Any template IDs that are lagging, and the offset to catch them up to, if defined;
    *         otherwise everything is fine.
    */
  def laggingOffsets[CtId <: domain.TemplateId.RequiredPkg](
      parties: Set[domain.Party],
      expectedOffset: domain.Offset,
      templateIds: NonEmpty[Set[CtId]],
  )(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver.TC,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[Option[(domain.Offset, Set[CtId])]] = {
    type Unsynced[Party, Off] = Map[Queries.SurrogateTpId, Map[Party, Off]]
    import scalaz.syntax.traverse._
    import sjd.q.queries.unsyncedOffsets
    for {
      tpids <- {
        import Queries.CompatImplicits.monadFromCatsMonad
        templateIds.toVector.toNEF.traverse { trp => surrogateTemplateId(trp) map ((_, trp)) }
      }: ConnectionIO[NonEmptyF[Vector, (SurrogateTpId, CtId)]]
      surrogatesToDomains = tpids.toMap
      unsyncedRaw <- unsyncedOffsets(
        domain.Offset unwrap expectedOffset,
        surrogatesToDomains.keySet,
      )
      unsynced = domain.Party.subst[Unsynced[*, domain.Offset], String](
        domain.Offset.tag.subst[Unsynced[String, *], String](unsyncedRaw)
      ): Unsynced[domain.Party, domain.Offset]
    } yield minimumViableOffsets(parties, surrogatesToDomains, expectedOffset, unsynced)
  }

  // postprocess the output of unsyncedOffsets
  private[dbbackend] def minimumViableOffsets[ITpId, OTpId, Party, Off: Order](
      queriedParty: Set[Party],
      surrogatesToDomains: ITpId => OTpId,
      expectedOffset: Off,
      unsynced: Map[ITpId, Map[Party, Off]],
  ): Option[(Off, Set[OTpId])] = {
    import scalaz.syntax.foldable1.{ToFoldableOps => _, _}
    import scalaz.std.iterable._
    val lagging: Option[Lagginess[ITpId, Off]] =
      (unsynced: Iterable[(ITpId, Map[Party, Off])]) foldMap1Opt {
        case (surrogateTpId, partyOffs) =>
          val maxLocalOff = partyOffs
            .collect {
              case (unsyncedParty, unsyncedOff)
                  if queriedParty(unsyncedParty) || unsyncedOff > expectedOffset =>
                unsyncedOff
            }
            .maximum
            .getOrElse(expectedOffset)
          val singleton = Set(surrogateTpId)
          // if a queried party is not in the map, we can safely assume that
          // it is exactly at expectedOffset
          val caughtUp = maxLocalOff <= expectedOffset ||
            queriedParty.forall { qp => partyOffs.get(qp).exists(_ === maxLocalOff) }
          Lagginess(
            if (caughtUp) singleton else Set.empty,
            if (caughtUp) Set.empty else singleton,
            // also an artifact of assuming that you actually ran update
            // to expectedOffset before using this function
            maxLocalOff max expectedOffset,
          )
      }
    lagging match {
      case Some(lagging) if lagging.maxOff > expectedOffset =>
        Some((lagging.maxOff, lagging.notCaughtUp map surrogatesToDomains))
      case _ => None
    }
  }

  private[dbbackend] final case class Lagginess[TpId, +Off](
      caughtUp: Set[TpId],
      notCaughtUp: Set[TpId],
      maxOff: Off,
  )
  private[dbbackend] object Lagginess {
    implicit def semigroup[TpId, Off: Order]: Semigroup[Lagginess[TpId, Off]] =
      Semigroup instance { case (Lagginess(cA, ncA, offA), Lagginess(cB, ncB, offB)) =>
        import scalaz.Ordering.{LT, GT, EQ}
        val (c, nc) = offA cmp offB match {
          case EQ => (cA union cB, Set.empty[TpId])
          case LT => (cB, cA)
          case GT => (cA, cB)
        }
        Lagginess(
          c,
          nc union ncA union ncB,
          offA max offB,
        )
      }

    implicit def equal[TpId: Order, Off: Equal]: Equal[Lagginess[TpId, Off]] =
      new Equal[Lagginess[TpId, Off]] {
        override def equalIsNatural = Equal[TpId].equalIsNatural && Equal[Off].equalIsNatural
        override def equal(a: Lagginess[TpId, Off], b: Lagginess[TpId, Off]) =
          if (equalIsNatural) a == b
          else
            a match {
              case Lagginess(caughtUp, notCaughtUp, maxOff) =>
                caughtUp === b.caughtUp && notCaughtUp === b.notCaughtUp && maxOff === b.maxOff
            }
      }
  }

  def updateOffset(
      parties: domain.PartySet,
      templateId: domain.TemplateId.RequiredPkg,
      newOffset: domain.Offset,
      lastOffsets: Map[domain.Party, domain.Offset],
  )(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver.TC,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[Unit] = {
    import cats.implicits._
    import sjd.q.queries
    import scalaz.syntax.foldable._
    val partyVector = domain.Party.unsubst(parties.toVector: Vector[domain.Party])
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
      parties: domain.PartySet,
      templateId: domain.ContractTypeId.Resolved,
      predicate: doobie.Fragment,
  )(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver.TC,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
    import sjd.q.queries
    for {
      tpId <- surrogateTemplateId(templateId)

      dbContracts <- queries
        .selectContracts(queriesPartySet(parties), tpId, predicate)
        .to[Vector]
      domainContracts = dbContracts.map(toDomain(templateId))
    } yield domainContracts
  }

  private[http] def selectContractsMultiTemplate[Pos](
      parties: domain.PartySet,
      predicates: Seq[(domain.ContractTypeId.Resolved, doobie.Fragment)],
      trackMatchIndices: MatchedQueryMarker[Pos],
  )(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver.TC,
      lc: LoggingContextOf[InstanceUUID],
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
                  queriesPartySet(parties),
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
                  queriesPartySet(parties),
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
      parties: domain.PartySet,
      templateId: domain.TemplateId.Resolved,
      contractId: domain.ContractId,
  )(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver.TC,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[Option[domain.ActiveContract[JsValue]]] = {
    import sjd.q._
    for {
      tpId <- surrogateTemplateId(templateId)
      dbContracts <- queries.fetchById(
        queriesPartySet(parties),
        tpId,
        domain.ContractId unwrap contractId,
      )
    } yield dbContracts.map(toDomain(templateId))
  }

  private[http] def fetchByKey(
      parties: domain.PartySet,
      templateId: domain.TemplateId.Resolved,
      key: Hash,
  )(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver.TC,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[Option[domain.ActiveContract[JsValue]]] = {
    import sjd.q._
    for {
      tpId <- surrogateTemplateId(templateId)
      dbContracts <- queries.fetchByKey(queriesPartySet(parties), tpId, key)
    } yield dbContracts.map(toDomain(templateId))
  }

  private[this] def surrogateTemplateId(templateId: domain.TemplateId.RequiredPkg)(implicit
      log: LogHandler,
      sjd: SupportedJdbcDriver.TC,
      lc: LoggingContextOf[InstanceUUID],
  ) =
    sjd.q.queries.surrogateTemplateId(
      templateId.packageId,
      templateId.moduleName,
      templateId.entityName,
    )

  private def toDomain(templateId: domain.ContractTypeId.Resolved)(
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
      parties: domain.PartySet,
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
