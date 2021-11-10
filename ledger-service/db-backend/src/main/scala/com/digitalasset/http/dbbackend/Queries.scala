// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.daml.scalautil.nonempty
import nonempty.{NonEmpty, +-:}
import nonempty.NonEmptyReturningOps._

import doobie._
import doobie.implicits._
import scala.collection.immutable.{Iterable, Seq => ISeq}
import scalaz.{@@, Functor, OneAnd, Tag}
import scalaz.Id.Id
import scalaz.syntax.foldable._
import scalaz.syntax.functor._
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
  import Queries._, InitDdl._
  import Implicits._

  protected[this] def dropTableIfExists(table: String): Fragment

  /** for use when generating predicates */
  private[http] val contractColumnName: Fragment = sql"payload"

  private[this] val createContractsTable = CreateTable(
    "contract",
    sql"""
      CREATE TABLE
        contract
        (contract_id """ ++ textType ++ sql""" NOT NULL PRIMARY KEY
        ,tpid """ ++ bigIntType ++ sql""" NOT NULL REFERENCES template_id (tpid)
        ,""" ++ jsonColumn(sql"key") ++ sql"""
        ,""" ++ jsonColumn(contractColumnName) ++
      contractsTableSignatoriesObservers ++ sql"""
        ,agreement_text """ ++ agreementTextType ++ sql"""
        )
    """,
  )

  protected[this] def contractsTableSignatoriesObservers: Fragment

  private[this] val indexContractsTable = CreateIndex(sql"""
      CREATE INDEX contract_tpid_idx ON contract (tpid)
    """)

  private[this] val createOffsetTable = CreateTable(
    "ledger_offset",
    sql"""
      CREATE TABLE
        ledger_offset
        (party """ ++ textType ++ sql""" NOT NULL
        ,tpid """ ++ bigIntType ++ sql""" NOT NULL REFERENCES template_id (tpid)
        ,last_offset """ ++ textType ++ sql""" NOT NULL
        ,PRIMARY KEY (party, tpid)
        )
    """,
  )

  protected[this] def bigIntType: Fragment // must match bigserial
  protected[this] def bigSerialType: Fragment
  protected[this] def textType: Fragment
  protected[this] def agreementTextType: Fragment

  // The max list size that can be used in `IN` clauses
  protected[this] def maxListSize: Option[Int]

  protected[this] def jsonColumn(name: Fragment): Fragment

  private[this] val createTemplateIdsTable = CreateTable(
    "template_id",
    sql"""
      CREATE TABLE
        template_id
        (tpid """ ++ bigSerialType ++ sql""" NOT NULL PRIMARY KEY
        ,package_id """ ++ textType ++ sql""" NOT NULL
        ,template_module_name """ ++ textType ++ sql""" NOT NULL
        ,template_entity_name """ ++ textType ++ sql""" NOT NULL
        ,UNIQUE (package_id, template_module_name, template_entity_name)
        )
    """,
  )

  private[http] def dropAllTablesIfExist(implicit log: LogHandler): ConnectionIO[Unit] = {
    import cats.instances.vector._, cats.syntax.foldable.{toFoldableOps => ToFoldableOps}
    initDatabaseDdls.reverse
      .collect { case CreateTable(name, _) => dropTableIfExists(name) }
      .traverse_(_.update.run)
  }

  protected[this] def initDatabaseDdls: Vector[InitDdl] =
    Vector(
      createTemplateIdsTable,
      createOffsetTable,
      createContractsTable,
      indexContractsTable,
    )

  private[http] def initDatabase(implicit log: LogHandler): ConnectionIO[Unit] = {
    import cats.instances.vector._, cats.syntax.foldable.{toFoldableOps => ToFoldableOps}
    initDatabaseDdls.traverse_(_.create.update.run)
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

  final def lastOffset(parties: OneAnd[Set, String], tpid: SurrogateTpId)(implicit
      log: LogHandler
  ): ConnectionIO[Map[String, String]] = {
    val partyVector =
      cats.data.OneAnd(parties.head, parties.tail.toList)
    val q = sql"""
      SELECT party, last_offset FROM ledger_offset WHERE tpid = $tpid AND
    """ ++ Fragments.in(fr"party", partyVector)
    q.query[(String, String)]
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
  )(implicit log: LogHandler): ConnectionIO[Int] = {
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
    val update = existingParties match {
      case hdP +: tlP =>
        Some(
          sql"""UPDATE ledger_offset SET last_offset = $newOffset
            WHERE """ ++ Fragments.in(fr"party", cats.data.OneAnd(hdP, tlP)) ++
            sql""" AND tpid = $tpid
                   AND last_offset = """ ++ caseLookup(
              lastOffsets.filter { case (k, _) => existingParties contains k },
              fr"party",
            )
        )
      case _ => None
    }
    for {
      inserted <-
        if (newParties.isEmpty) { Applicative[ConnectionIO].pure(0) }
        else {
          insert.updateMany(newParties.toList.map(p => (p, tpid, newOffset)))
        }
      updated <- update.cata(_.update.run, Applicative[ConnectionIO].pure(0))
    } yield { inserted + updated }
  }

  private[this] def caseLookup(m: Map[String, String], selector: Fragment): Fragment =
    fr"CASE" ++ {
      assert(m.nonEmpty, "existing offsets must be non-empty")
      val when +: whens = m.iterator.map { case (k, v) =>
        fr"WHEN (" ++ selector ++ fr" = $k) THEN $v"
      }.toVector
      concatFragment(OneAnd(when, whens))
    } ++ fr"ELSE NULL END"

  // different databases encode contract keys in different formats
  protected[this] type DBContractKey
  protected[this] def toDBContractKey[CK: JsonWriter](ck: CK): DBContractKey

  final def insertContracts[F[_]: cats.Foldable: Functor, CK: JsonWriter, PL: JsonWriter](
      dbcs: F[DBContract[SurrogateTpId, CK, PL, Seq[String]]]
  )(implicit log: LogHandler, pas: Put[Array[String]]): ConnectionIO[Int] =
    primInsertContracts(dbcs.map(_.mapKeyPayloadParties(toDBContractKey(_), _.toJson, _.toArray)))

  protected[this] def primInsertContracts[F[_]: cats.Foldable: Functor](
      dbcs: F[DBContract[SurrogateTpId, DBContractKey, JsValue, Array[String]]]
  )(implicit log: LogHandler, pas: Put[Array[String]]): ConnectionIO[Int]

  final def deleteContracts(
      cids: Set[String]
  )(implicit log: LogHandler): ConnectionIO[Int] = {
    import cats.data.NonEmptyVector
    import cats.instances.vector._
    import cats.instances.int._
    import cats.syntax.foldable._
    NonEmptyVector.fromVector(cids.toVector) match {
      case None =>
        free.connection.pure(0)
      case Some(cids) =>
        val chunks = maxListSize.fold(Vector(cids)) { size =>
          require(size >= 1, s"size=$size but must be positive")
          cids.toVector.grouped(size).map(NonEmptyVector.fromVectorUnsafe).toVector
        }
        chunks
          .map(chunk =>
            (fr"DELETE FROM contract WHERE " ++ Fragments.in(fr"contract_id", chunk)).update.run
          )
          .foldA
    }
  }

  private[http] final def selectContracts(
      parties: OneAnd[Set, String],
      tpid: SurrogateTpId,
      predicate: Fragment,
  )(implicit
      log: LogHandler,
      gvs: Get[Vector[String]],
      pvs: Put[Vector[String]],
  ): Query0[DBContract[Unit, JsValue, JsValue, Vector[String]]] =
    selectContractsMultiTemplate(parties, ISeq((tpid, predicate)), MatchedQueryMarker.Unused)
      .map(_ copy (templateId = ()))

  /** Make the smallest number of queries from `queries` that still indicates
    * which query or queries produced each contract.
    *
    * A contract cannot be produced more than once from a given resulting query,
    * but may be produced more than once from different queries.  In each case, the
    * `templateId` of the resulting [[DBContract]] is actually the 0-based index
    * into the `queries` argument that produced the contract.
    */
  private[http] def selectContractsMultiTemplate[T[_], Mark](
      parties: OneAnd[Set, String],
      queries: ISeq[(SurrogateTpId, Fragment)],
      trackMatchIndices: MatchedQueryMarker[T, Mark],
  )(implicit
      log: LogHandler,
      gvs: Get[Vector[String]],
      pvs: Put[Vector[String]],
  ): T[Query0[DBContract[Mark, JsValue, JsValue, Vector[String]]]]

  private[http] final def fetchById(
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

  private[dbbackend] sealed abstract class InitDdl extends Product with Serializable {
    def create: Fragment
  }

  private[dbbackend] object InitDdl {
    final case class CreateTable(name: String, create: Fragment) extends InitDdl
    final case class CreateIndex(create: Fragment) extends InitDdl
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
  private[dbbackend] def uniqueSets[A, B](iter: Iterable[(A, B)]): Seq[NonEmpty[Map[A, B]]] =
    unfold(
      iter
        .groupBy1(_._1)
        .transform((_, i) => i.toList): Map[A, NonEmpty[List[(_, B)]]]
    ) {
      case NonEmpty(m) =>
        Some {
          val hd = m transform { (_, abs) =>
            val (_, b) +-: _ = abs
            b
          }
          val tl = m collect { case (a, _ +-: NonEmpty(tl)) => (a, tl) }
          (hd, tl)
        }
      case _ => None
    }

  private[http] val Postgres: Queries = PostgresQueries
  private[http] val Oracle: Queries = OracleQueries
}

private object PostgresQueries extends Queries {
  import Queries._, Queries.InitDdl.CreateIndex
  import Implicits._

  protected[this] override def dropTableIfExists(table: String) =
    Fragment.const(s"DROP TABLE IF EXISTS ${table}")

  protected[this] override def bigIntType = sql"BIGINT"
  protected[this] override def bigSerialType = sql"BIGSERIAL"
  protected[this] override def textType = sql"TEXT"
  protected[this] override def agreementTextType = sql"TEXT NOT NULL"

  protected[this] override def jsonColumn(name: Fragment) = name ++ sql" JSONB NOT NULL"

  protected[this] override val maxListSize = None

  private[this] val indexContractsKeys = CreateIndex(sql"""
      CREATE INDEX contract_tpid_key_idx ON contract USING BTREE (tpid, key)
  """)

  protected[this] override def initDatabaseDdls = super.initDatabaseDdls :+ indexContractsKeys

  protected[this] override def contractsTableSignatoriesObservers = sql"""
    ,signatories TEXT ARRAY NOT NULL
    ,observers TEXT ARRAY NOT NULL
  """

  protected[this] type DBContractKey = JsValue

  protected[this] override def toDBContractKey[CK: JsonWriter](x: CK) = x.toJson

  protected[this] override def primInsertContracts[F[_]: cats.Foldable: Functor](
      dbcs: F[DBContract[SurrogateTpId, DBContractKey, JsValue, Array[String]]]
  )(implicit log: LogHandler, pas: Put[Array[String]]): ConnectionIO[Int] =
    Update[DBContract[SurrogateTpId, JsValue, JsValue, Array[String]]](
      """
        INSERT INTO contract
        VALUES (?, ?, ?::jsonb, ?::jsonb, ?, ?, ?)
        ON CONFLICT (contract_id) DO NOTHING
      """,
      logHandler0 = log,
    ).updateMany(dbcs)

  private[http] override def selectContractsMultiTemplate[T[_], Mark](
      parties: OneAnd[Set, String],
      queries: ISeq[(SurrogateTpId, Fragment)],
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
          preds: NonEmpty[Map[SurrogateTpId, (Fragment, Ix)]] =>
            val predHd +-: predTl = preds.toVector
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
}

private object OracleQueries extends Queries {
  import Queries.{DBContract, MatchedQueryMarker, SurrogateTpId}, Queries.InitDdl.CreateTable
  import Implicits._

  protected[this] override def dropTableIfExists(table: String) = sql"""BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE ' || $table;
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
          RAISE;
        END IF;
    END;"""

  protected[this] override def bigIntType = sql"NUMBER(19,0)"
  protected[this] override def bigSerialType =
    bigIntType ++ sql" GENERATED ALWAYS AS IDENTITY"
  // TODO SC refine the string formats chosen here and for jsonColumn
  protected[this] override def textType = sql"NVARCHAR2(100)"
  protected[this] override def agreementTextType = sql"NVARCHAR2(100)"

  protected[this] override def jsonColumn(name: Fragment) =
    name ++ sql" CLOB NOT NULL CONSTRAINT ensure_json_" ++ name ++ sql" CHECK (" ++ name ++ sql" IS JSON)"

  // See http://www.dba-oracle.com/t_ora_01795_maximum_number_of_expressions_in_a_list_is_1000.htm
  protected[this] override def maxListSize = Some(1000)

  protected[this] override def contractsTableSignatoriesObservers = sql""

  private val createSignatoriesTable = CreateTable(
    "signatories",
    sql"""
      CREATE TABLE
        signatories
          (contract_id NVARCHAR2(100) NOT NULL REFERENCES contract(contract_id) ON DELETE CASCADE
          ,party NVARCHAR2(100) NOT NULL
          ,UNIQUE (contract_id, party)
          )
    """,
  )

  private val createObserversTable = CreateTable(
    "observers",
    sql"""
      CREATE TABLE
        observers
          (contract_id NVARCHAR2(100) NOT NULL REFERENCES contract(contract_id) ON DELETE CASCADE
          ,party NVARCHAR2(100) NOT NULL
          ,UNIQUE (contract_id, party)
          )
    """,
  )

  protected[this] override def initDatabaseDdls =
    super.initDatabaseDdls ++ Seq(createSignatoriesTable, createObserversTable)

  protected[this] type DBContractKey = JsValue

  protected[this] override def toDBContractKey[CK: JsonWriter](x: CK) =
    JsObject(Map("key" -> x.toJson))

  protected[this] override def primInsertContracts[F[_]: cats.Foldable: Functor](
      dbcs: F[DBContract[SurrogateTpId, DBContractKey, JsValue, Array[String]]]
  )(implicit log: LogHandler, pas: Put[Array[String]]): ConnectionIO[Int] = {
    println("insert contracts")
    println(dbcs)
    val r = Update[(String, SurrogateTpId, JsValue, JsValue, String)](
      """
        INSERT INTO contract(contract_id, tpid, key, payload, agreement_text)
        VALUES (?, ?, ?, ?, ?)
      """,
      logHandler0 = log,
    ).updateMany(
      dbcs
        .map { c =>
//          println(c)
          (c.contractId, c.templateId, c.key, c.payload, c.agreementText)
        }
    )
    println("inserted")
    import cats.syntax.foldable._, cats.instances.vector._
    val r2 = Update[(String, String)](
      """
        INSERT INTO signatories(contract_id, party)
        VALUES (?, ?)
      """,
      logHandler0 = log,
    ).updateMany(dbcs.foldMap(c => c.signatories.view.map(s => (c.contractId, s)).toVector))
    val r3 = Update[(String, String)](
      """
        INSERT INTO observers(contract_id, party)
        VALUES (?, ?)
      """,
      logHandler0 = log,
    ).updateMany(dbcs.foldMap(c => c.observers.view.map(s => (c.contractId, s)).toVector))
    r *> r2 *> r3
  }

  private[http] override def selectContractsMultiTemplate[T[_], Mark](
      parties: OneAnd[Set, String],
      queries: ISeq[(SurrogateTpId, Fragment)],
      trackMatchIndices: MatchedQueryMarker[T, Mark],
  )(implicit
      log: LogHandler,
      gvs: Get[Vector[String]],
      pvs: Put[Vector[String]],
  ): T[Query0[DBContract[Mark, JsValue, JsValue, Vector[String]]]] = {
    val Seq((tpid, predicate @ _)) = queries // TODO SC handle more than one
    val _ = parties
    println("selecting")
    val q = sql"""SELECT contract_id, key, payload, agreement_text
                  FROM contract
                  WHERE tpid = $tpid""" // TODO SC AND (""" ++ predicate ++ sql")"
    trackMatchIndices match {
      case MatchedQueryMarker.ByInt => sys.error("TODO websocket Oracle support")
      case MatchedQueryMarker.Unused =>
        q.query[(String, JsValue, JsValue, Option[String])].map {
          case (cid, key, payload, agreement) =>
            DBContract(
              contractId = cid,
              templateId = tpid,
              key = key,
              payload = payload,
              signatories = Vector(),
              observers = Vector(),
              agreementText = agreement getOrElse "",
            )
        }
    }
  }
}
