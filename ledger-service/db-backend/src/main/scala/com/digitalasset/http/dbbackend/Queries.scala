// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.dbbackend

import scala.language.higherKinds
import com.github.ghik.silencer.silent

import doobie._
import doobie.implicits._
import scalaz.{@@, Foldable, Functor, OneAnd, Tag}
import scalaz.syntax.foldable._
import scalaz.syntax.functor._
import scalaz.syntax.std.option._
import spray.json._
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.functor._

object Queries {
  import Implicits._

  def dropTableIfExists(table: String): Fragment = Fragment.const(s"DROP TABLE IF EXISTS ${table}")

  // NB: #, order of arguments must match createContractsTable
  final case class DBContract[+TpId, +CA, +WP](
      contractId: String,
      templateId: TpId,
      createArguments: CA,
      witnessParties: WP) {
    def mapTemplateId[B](f: TpId => B): DBContract[B, CA, WP] = copy(templateId = f(templateId))
    def mapArgsParties[B, C](f: CA => B, g: WP => C): DBContract[TpId, B, C] =
      copy(createArguments = f(createArguments), witnessParties = g(witnessParties))
  }

  /** for use when generating predicates */
  private[http] val contractColumnName: Fragment = sql"create_arguments"

  val dropContractsTable: Fragment = dropTableIfExists("contract")

  val createContractsTable: Fragment = sql"""
      CREATE TABLE
        contract
        (contract_id TEXT PRIMARY KEY NOT NULL
        ,tpid BIGINT NOT NULL REFERENCES template_id (tpid)
        ,create_arguments JSONB NOT NULL
        ,witness_parties TEXT ARRAY NOT NULL
        )
    """

  val indexContractsTable: Fragment = sql"""
      CREATE INDEX ON contract (tpid)
    """

  final case class DBOffset[+TpId](party: String, templateId: TpId, lastOffset: String)

  val dropOffsetTable: Fragment = dropTableIfExists("ledger_offset")

  val createOffsetTable: Fragment = sql"""
      CREATE TABLE
        ledger_offset
        (party TEXT NOT NULL
        ,tpid BIGINT NOT NULL REFERENCES template_id (tpid)
        ,last_offset TEXT NOT NULL
        ,PRIMARY KEY (party, tpid)
        )
    """

  sealed trait SurrogateTpIdTag
  val SurrogateTpId = Tag.of[SurrogateTpIdTag]
  type SurrogateTpId = Long @@ SurrogateTpIdTag // matches tpid (BIGINT) below

  val dropTemplateIdsTable: Fragment = dropTableIfExists("template_id")

  val createTemplateIdsTable: Fragment = sql"""
      CREATE TABLE
        template_id
        (tpid BIGSERIAL PRIMARY KEY NOT NULL
        ,package_id TEXT NOT NULL
        ,template_module_name TEXT NOT NULL
        ,template_entity_name TEXT NOT NULL
        ,UNIQUE (package_id, template_module_name, template_entity_name)
        )
    """

  private[http] def dropAllTablesIfExist(implicit log: LogHandler): ConnectionIO[Unit] =
    (dropContractsTable.update.run
      *> dropOffsetTable.update.run
      *> dropTemplateIdsTable.update.run).void

  private[http] def initDatabase(implicit log: LogHandler): ConnectionIO[Unit] =
    (createTemplateIdsTable.update.run
      *> createOffsetTable.update.run
      *> createContractsTable.update.run
      *> indexContractsTable.update.run).void

  def surrogateTemplateId(packageId: String, moduleName: String, entityName: String)(
      implicit log: LogHandler): ConnectionIO[SurrogateTpId] =
    sql"""SELECT tpid FROM template_id
          WHERE (package_id = $packageId AND template_module_name = $moduleName
                 AND template_entity_name = $entityName)"""
      .query[SurrogateTpId]
      .option flatMap {
      _.cata(
        _.pure[ConnectionIO],
        sql"""INSERT INTO template_id (package_id, template_module_name, template_entity_name)
              VALUES ($packageId, $moduleName, $entityName)""".update
          .withUniqueGeneratedKeys[SurrogateTpId]("tpid")
      )
    }

  def lastOffset(party: String, tpid: SurrogateTpId)(
      implicit log: LogHandler): ConnectionIO[Option[String]] =
    sql"""SELECT last_offset FROM ledger_offset WHERE (party = $party AND tpid = $tpid)"""
      .query[String]
      .option

  /** Consistency of the whole database mostly pivots around the offset update
    * check, since an offset read and write bookend the update.
    *
    * Considering two concurrent transactions, A and B:
    *
    * If both insert, you get a uniqueness violation.
    * When A updates, the row locks until commit, so B's update waits for that commit.
    * At that point the result depends on isolation level:
    *   - read committed: update count 0 (caller must catch this; json-api rolls back on this)
    *   - repeatable read or serializable: "could not serialize access due to concurrent update"
    *     error on update
    *
    * If A inserts but B updates, the transactions are sufficiently serialized that
    * there are no logical conflicts.
    */
  private[http] def updateOffset(
      party: String,
      tpid: SurrogateTpId,
      newOffset: String,
      lastOffsetO: Option[String])(implicit log: LogHandler): ConnectionIO[Int] =
    lastOffsetO.cata(
      lastOffset =>
        sql"""UPDATE ledger_offset SET last_offset = $newOffset where party = $party AND tpid = $tpid AND last_offset = $lastOffset""".update.run,
      sql"""INSERT INTO ledger_offset VALUES ($party, $tpid, $newOffset)""".update.run
    )

  @silent // pas is demonstrably used; try taking it out
  def insertContracts[F[_]: cats.Foldable: Functor, CA: JsonWriter](
      dbcs: F[DBContract[SurrogateTpId, CA, Seq[String]]])(
      implicit log: LogHandler,
      pas: Put[Array[String]]): ConnectionIO[Int] =
    Update[DBContract[SurrogateTpId, JsValue, Array[String]]](
      """
        INSERT INTO contract
        VALUES (?, ?, ?::jsonb, ?)
        ON CONFLICT (contract_id) DO NOTHING
      """,
      logHandler0 = log).updateMany(dbcs.map(_.mapArgsParties(_.toJson, _.toArray)))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def deleteContracts[F[_]: Foldable](cids: F[String])(
      implicit log: LogHandler): ConnectionIO[Int] = {
    cids.toVector match {
      case Vector(hd, tl @ _*) =>
        (sql"DELETE FROM contract WHERE contract_id IN ("
          ++ concatFragment(OneAnd(sql"$hd", tl.toIndexedSeq map (cid => sql", $cid")))
          ++ sql")").update.run
      case _ => free.connection.pure(0)
    }
  }

  private[http] def concatFragment[F[X] <: IndexedSeq[X]](xs: OneAnd[F, Fragment]): Fragment = {
    val OneAnd(hd, tl) = xs
    def go(s: Int, e: Int): Fragment =
      (e - s: @annotation.switch) match {
        case 0 => sql""
        case 1 => tl(s)
        case 2 => tl(s) ++ tl(s + 1)
        case n =>
          val pivot = s + n / 2
          go(s, pivot) ++ go(pivot, e)
      }
    hd ++ go(0, tl.size)
  }

  private[http] def selectContracts(
      party: String,
      tpid: SurrogateTpId,
      predicate: Fragment): Query0[DBContract[Unit, JsValue, Unit]] = {
    val q = sql"""SELECT (contract_id, create_arguments)
                  FROM contract
                  WHERE witness_parties @> ARRAY[$party] AND tpid = $tpid
                        AND (""" ++ predicate ++ sql")"
    q.query[(String, JsValue)].map {
      case (cid, ca) => DBContract(cid, (), ca, ())
    }
  }

  object Implicits {
    implicit val `JsValue put`: Meta[JsValue] =
      Meta[String].timap(_.parseJson)(_.compactPrint)

    implicit val `SurrogateTpId meta`: Meta[SurrogateTpId] =
      SurrogateTpId subst Meta[Long]
  }
}
