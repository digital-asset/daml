// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import scala.language.higherKinds
import com.github.ghik.silencer.silent

import doobie._
import doobie.implicits._
import scalaz.{@@, Foldable, Functor, OneAnd, Tag}
import scalaz.Id.Id
import scalaz.syntax.foldable._
import scalaz.syntax.functor._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.std.stream.unfold
import scalaz.std.AllInstances._
import spray.json._
import cats.instances.list._
import cats.Applicative
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.functor._

sealed abstract class Queries {
  import Queries._
  import Implicits._

  def dropTableIfExists(table: String): Fragment = Fragment.const(s"DROP TABLE IF EXISTS ${table}")

  /** for use when generating predicates */
  private[http] val contractColumnName: Fragment = sql"payload"

  private[this] val dropContractsTable: Fragment = dropTableIfExists("contract")

  private[this] val createContractsTable: Fragment = sql"""
      CREATE TABLE
        contract
        (contract_id TEXT NOT NULL PRIMARY KEY
        ,tpid BIGINT NOT NULL REFERENCES template_id (tpid)
        ,key JSONB NOT NULL
        ,payload JSONB NOT NULL
        ,signatories TEXT ARRAY NOT NULL
        ,observers TEXT ARRAY NOT NULL
        ,agreement_text TEXT NOT NULL
        )
    """

  val indexContractsTable: Fragment = sql"""
      CREATE INDEX contract_tpid_idx ON contract (tpid)
    """

  private[this] val indexContractsKeys: Fragment = sql"""
      CREATE INDEX contract_tpid_key_idx ON contract USING BTREE (tpid, key)
  """

  private[this] val dropOffsetTable: Fragment = dropTableIfExists("ledger_offset")

  private[this] val createOffsetTable: Fragment = sql"""
      CREATE TABLE
        ledger_offset
        (party TEXT NOT NULL
        ,tpid BIGINT NOT NULL REFERENCES template_id (tpid)
        ,last_offset TEXT NOT NULL
        ,PRIMARY KEY (party, tpid)
        )
    """

  private[this] val dropTemplateIdsTable: Fragment = dropTableIfExists("template_id")

  private[this] val createTemplateIdsTable: Fragment = sql"""
      CREATE TABLE
        template_id
        (tpid BIGSERIAL NOT NULL PRIMARY KEY
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

  private[this] def initDatabaseDdls: Vector[Fragment] =
    Vector(
      createTemplateIdsTable,
      createOffsetTable,
      createContractsTable,
      indexContractsTable,
      indexContractsKeys,
    )

  private[http] def initDatabase(implicit log: LogHandler): ConnectionIO[Unit] = {
    import cats.instances.vector._, cats.syntax.foldable.{toFoldableOps => ToFoldableOps}
    initDatabaseDdls.traverse_(_.update.run)
  }

  def surrogateTemplateId(packageId: String, moduleName: String, entityName: String)(implicit
      log: LogHandler
  ): ConnectionIO[SurrogateTpId] =
    sql"""SELECT tpid FROM template_id
          WHERE (package_id = $packageId AND template_module_name = $moduleName
                 AND template_entity_name = $entityName)"""
      .query[SurrogateTpId]
      .option flatMap {
      _.cata(
        _.pure[ConnectionIO],
        sql"""INSERT INTO template_id (package_id, template_module_name, template_entity_name)
              VALUES ($packageId, $moduleName, $entityName)""".update
          .withUniqueGeneratedKeys[SurrogateTpId]("tpid"),
      )
    }

  def lastOffset(parties: OneAnd[Set, String], tpid: SurrogateTpId)(implicit
      log: LogHandler,
      pls: Put[Vector[String]],
  ): ConnectionIO[Map[String, String]] = {
    val partyVector = parties.toVector
    sql"""SELECT party, last_offset FROM ledger_offset WHERE (party = ANY(${partyVector}) AND tpid = $tpid)"""
      .query[(String, String)]
      .to[Vector]
      .map(_.toMap)
  }

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
  private[http] def updateOffset[F[_]: cats.Foldable](
      parties: F[String],
      tpid: SurrogateTpId,
      newOffset: String,
      lastOffsets: Map[String, String],
  )(implicit log: LogHandler, pls: Put[List[String]]): ConnectionIO[Int] = {
    import spray.json.DefaultJsonProtocol._
    val (existingParties, newParties) = {
      import cats.syntax.foldable._
      parties.toList.partition(p => lastOffsets.contains(p))
    }
    // If a concurrent transaction inserted an offset for a new party, the insert will fail.
    val insert = Update[(String, SurrogateTpId, String)](
      """INSERT INTO ledger_offset VALUES(?, ?, ?)""",
      logHandler0 = log,
    )
    // If a concurrent transaction updated the offset for an existing party, we will get
    // fewer rows and throw a StaleOffsetException in the caller.
    val update =
      sql"""UPDATE ledger_offset SET last_offset = $newOffset WHERE party = ANY($existingParties::text[]) AND tpid = $tpid AND last_offset = (${lastOffsets.toJson}::jsonb->>party)"""
    for {
      inserted <-
        if (newParties.empty) { Applicative[ConnectionIO].pure(0) }
        else {
          insert.updateMany(newParties.toList.map(p => (p, tpid, newOffset)))
        }
      updated <-
        if (existingParties.empty) { Applicative[ConnectionIO].pure(0) }
        else {
          update.update.run
        }
    } yield { inserted + updated }
  }

  @silent(" pas .* never used")
  def insertContracts[F[_]: cats.Foldable: Functor, CK: JsonWriter, PL: JsonWriter](
      dbcs: F[DBContract[SurrogateTpId, CK, PL, Seq[String]]]
  )(implicit log: LogHandler, pas: Put[Array[String]]): ConnectionIO[Int] =
    Update[DBContract[SurrogateTpId, JsValue, JsValue, Array[String]]](
      """
        INSERT INTO contract
        VALUES (?, ?, ?::jsonb, ?::jsonb, ?, ?, ?)
        ON CONFLICT (contract_id) DO NOTHING
      """,
      logHandler0 = log,
    ).updateMany(dbcs.map(_.mapKeyPayloadParties(_.toJson, _.toJson, _.toArray)))

  def deleteContracts[F[_]: Foldable](
      cids: F[String]
  )(implicit log: LogHandler): ConnectionIO[Int] = {
    cids.toVector match {
      case Vector(hd, tl @ _*) =>
        (sql"DELETE FROM contract WHERE contract_id IN ("
          ++ concatFragment(OneAnd(sql"$hd", tl.toIndexedSeq map (cid => sql", $cid")))
          ++ sql")").update.run
      case _ => free.connection.pure(0)
    }
  }

  @silent(" gvs .* never used")
  private[http] def selectContracts(
      parties: OneAnd[Set, String],
      tpid: SurrogateTpId,
      predicate: Fragment,
  )(implicit
      log: LogHandler,
      gvs: Get[Vector[String]],
      pvs: Put[Vector[String]],
  ): Query0[DBContract[Unit, JsValue, JsValue, Vector[String]]] = {
    val partyVector = parties.toVector
    val q = sql"""SELECT contract_id, key, payload, signatories, observers, agreement_text
                  FROM contract
                  WHERE (signatories && $partyVector::text[] OR observers && $partyVector::text[])
                   AND tpid = $tpid AND (""" ++ predicate ++ sql")"
    q.query[(String, JsValue, JsValue, Vector[String], Vector[String], String)].map {
      case (cid, key, payload, signatories, observers, agreement) =>
        DBContract(
          contractId = cid,
          templateId = (),
          key = key,
          payload = payload,
          signatories = signatories,
          observers = observers,
          agreementText = agreement,
        )
    }
  }

  /** Make the smallest number of queries from `queries` that still indicates
    * which query or queries produced each contract.
    *
    * A contract cannot be produced more than once from a given resulting query,
    * but may be produced more than once from different queries.  In each case, the
    * `templateId` of the resulting [[DBContract]] is actually the 0-based index
    * into the `queries` argument that produced the contract.
    */
  @silent(" gvs .* never used")
  private[http] def selectContractsMultiTemplate[T[_], Mark](
      parties: OneAnd[Set, String],
      queries: Seq[(SurrogateTpId, Fragment)],
      trackMatchIndices: MatchedQueryMarker[T, Mark],
  )(implicit
      log: LogHandler,
      gvs: Get[Vector[String]],
      pvs: Put[Vector[String]],
  ): T[Query0[DBContract[Mark, JsValue, JsValue, Vector[String]]]] = {
    val partyVector = parties.toVector
    def query(preds: OneAnd[Vector, (SurrogateTpId, Fragment)], findMark: SurrogateTpId => Mark) = {
      val assocedPreds = preds.map { case (tpid, predicate) =>
        sql"(tpid = $tpid AND (" ++ predicate ++ sql"))"
      }
      val unionPred = concatFragment(intersperse(assocedPreds, sql" OR "))
      val q = sql"""SELECT contract_id, tpid, key, payload, signatories, observers, agreement_text
                      FROM contract AS c
                      WHERE (signatories && $partyVector::text[] OR observers && $partyVector::text[])
                       AND (""" ++ unionPred ++ sql")"
      q.query[(String, SurrogateTpId, JsValue, JsValue, Vector[String], Vector[String], String)]
        .map { case (cid, tpid, key, payload, signatories, observers, agreement) =>
          DBContract(
            contractId = cid,
            templateId = findMark(tpid),
            key = key,
            payload = payload,
            signatories = signatories,
            observers = observers,
            agreementText = agreement,
          )
        }
    }

    trackMatchIndices match {
      case MatchedQueryMarker.ByInt =>
        type Ix = Int
        uniqueSets(queries.zipWithIndex map { case ((tpid, pred), ix) => (tpid, (pred, ix)) }).map {
          preds: Map[SurrogateTpId, (Fragment, Ix)] =>
            val predHd +: predTl = preds.toVector
            val predsList = OneAnd(predHd, predTl).map { case (tpid, (predicate, _)) =>
              (tpid, predicate)
            }
            query(predsList, tpid => preds(tpid)._2)
        }

      case MatchedQueryMarker.Unused =>
        val predHd +: predTl = queries.toVector
        query(OneAnd(predHd, predTl), identity)
    }
  }

  private[http] def fetchById(
      parties: OneAnd[Set, String],
      tpid: SurrogateTpId,
      contractId: String,
  )(implicit
      log: LogHandler,
      gvs: Get[Vector[String]],
      pvs: Put[Vector[String]],
  ): ConnectionIO[Option[DBContract[Unit, JsValue, JsValue, Vector[String]]]] =
    selectContracts(parties, tpid, sql"contract_id = $contractId").option

  private[http] def fetchByKey(parties: OneAnd[Set, String], tpid: SurrogateTpId, key: JsValue)(
      implicit
      log: LogHandler,
      gvs: Get[Vector[String]],
      pvs: Put[Vector[String]],
  ): ConnectionIO[Option[DBContract[Unit, JsValue, JsValue, Vector[String]]]] =
    selectContracts(parties, tpid, sql"key = $key::jsonb").option

  private[http] def keyEquality(key: JsValue): Fragment =
    sql"key = $key::jsonb"

  object Implicits {
    implicit val `JsValue put`: Meta[JsValue] =
      Meta[String].timap(_.parseJson)(_.compactPrint)

    implicit val `SurrogateTpId meta`: Meta[SurrogateTpId] =
      SurrogateTpId subst Meta[Long]
  }
}

object Queries {
  sealed trait SurrogateTpIdTag
  val SurrogateTpId = Tag.of[SurrogateTpIdTag]
  type SurrogateTpId = Long @@ SurrogateTpIdTag // matches tpid (BIGINT) above

  // NB: #, order of arguments must match createContractsTable
  final case class DBContract[+TpId, +CK, +PL, +Prt](
      contractId: String,
      templateId: TpId,
      key: CK,
      payload: PL,
      signatories: Prt,
      observers: Prt,
      agreementText: String,
  ) {
    def mapTemplateId[B](f: TpId => B): DBContract[B, CK, PL, Prt] =
      copy(templateId = f(templateId))
    def mapKeyPayloadParties[A, B, C](
        f: CK => A,
        g: PL => B,
        h: Prt => C,
    ): DBContract[TpId, A, B, C] =
      copy(
        key = f(key),
        payload = g(payload),
        signatories = h(signatories),
        observers = h(observers),
      )
  }

  /** Whether selectContractsMultiTemplate computes a matchedQueries marker,
    * and whether it may compute >1 query to run.
    *
    * @tparam T The traversable of queries that result.
    * @tparam Mark The "marker" indicating which query matched.
    */
  private[http] sealed abstract class MatchedQueryMarker[T[_], +Mark]
      extends Product
      with Serializable
  private[http] object MatchedQueryMarker {
    case object ByInt extends MatchedQueryMarker[Seq, Int]
    case object Unused extends MatchedQueryMarker[Id, SurrogateTpId]
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

  private[http] def intersperse[A](oaa: OneAnd[Vector, A], a: A): OneAnd[Vector, A] =
    oaa.copy(tail = oaa.tail.flatMap(Vector(a, _)))

  // Like groupBy but split into n maps where n is the longest list under groupBy.
  // Invariant: every element of the result is non-empty
  private[dbbackend] def uniqueSets[A, B](iter: Iterable[(A, B)]): Seq[Map[A, B]] =
    unfold(iter.groupBy(_._1).transform((_, i) => i.toList): Map[A, List[(_, B)]]) { m =>
      // invariant: every value of m is non-empty
      m.nonEmpty option {
        val hd = m transform { (_, abs) =>
          val (_, b) +: _ = abs
          b
        }
        val tl = m collect { case (a, _ +: (tl @ (_ +: _))) => (a, tl) }
        (hd, tl)
      }
    }

  private[http] val Postgres: Queries = PostgresQueries
}

private object PostgresQueries extends Queries
