// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.daml.lf.data.Ref
import com.daml.scalautil.nonempty
import nonempty.{NonEmpty, +-:}
import nonempty.NonEmptyReturningOps._

import doobie._
import doobie.implicits._
import scala.annotation.nowarn
import scala.collection.compat._
import scala.collection.immutable.{Seq => ISeq, SortedMap}
import scalaz.{@@, Cord, Functor, OneAnd, Tag, \/, -\/, \/-}
import scalaz.Digit._0
import scalaz.syntax.foldable._
import scalaz.syntax.functor._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.syntax.std.string._
import scalaz.std.AllInstances._
import spray.json._
import cats.instances.list._
import cats.Applicative
import cats.syntax.applicative._
import cats.syntax.functor._
import com.daml.http.util.Logging.InstanceUUID
import com.daml.lf.crypto.Hash
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Metrics
import doobie.free.connection

sealed abstract class Queries(tablePrefix: String, tpIdCacheMaxEntries: Long)(implicit
    metrics: Metrics
) {
  import Queries.{Implicits => _, _}, InitDdl._
  import Queries.Implicits._

  val schemaVersion = 2

  private[http] val surrogateTpIdCache = new SurrogateTemplateIdCache(metrics, tpIdCacheMaxEntries)

  protected[this] def dropIfExists(drop: Droppable): Fragment

  /** for use when generating predicates */
  protected[this] val contractColumnName: Fragment = sql"payload"

  protected[this] val tablePrefixFr = Fragment.const0(tablePrefix)

  /** Table names with prefix * */

  val contractTableNameRaw = s"${tablePrefix}contract"
  val contractTableName: Fragment = Fragment.const0(contractTableNameRaw)

  val ledgerOffsetTableNameRaw = s"${tablePrefix}ledger_offset"
  val ledgerOffsetTableName: Fragment =
    Fragment.const0(ledgerOffsetTableNameRaw)

  val templateIdTableNameRaw = s"${tablePrefix}template_id"
  val templateIdTableName: Fragment = Fragment.const0(templateIdTableNameRaw)

  val jsonApiSchemaVersionTableNameRaw = s"${tablePrefix}json_api_schema_version"
  val jsonApiSchemaVersionTableName: Fragment =
    Fragment.const0(jsonApiSchemaVersionTableNameRaw)

  private[this] val createContractsTable = CreateTable(
    contractTableNameRaw,
    sql"""
      CREATE TABLE
        $contractTableName
        (contract_id $contractIdType NOT NULL CONSTRAINT ${tablePrefixFr}contract_k PRIMARY KEY
        ,tpid $bigIntType NOT NULL REFERENCES $templateIdTableName (tpid)
        ,${jsonColumn(sql"key")}
        ,key_hash $keyHashColumn
        ,${jsonColumn(contractColumnName)}
        $contractsTableSignatoriesObservers
        ,agreement_text $agreementTextType
        )
    """,
  )

  protected[this] def contractsTableSignatoriesObservers: Fragment

  private[this] val createOffsetTable = CreateTable(
    ledgerOffsetTableNameRaw,
    sql"""
      CREATE TABLE
        $ledgerOffsetTableName
        (party $partyType NOT NULL
        ,tpid $bigIntType NOT NULL REFERENCES $templateIdTableName (tpid)
        ,last_offset $offsetType NOT NULL
        ,PRIMARY KEY (party, tpid)
        )
    """,
  )

  protected[this] def bigIntType: Fragment // must match bigserial
  protected[this] def bigSerialType: Fragment
  protected[this] def packageIdType: Fragment
  protected[this] def partyOffsetContractIdType: Fragment
  protected[this] final def partyType = partyOffsetContractIdType
  private[this] def offsetType = partyOffsetContractIdType
  protected[this] final def contractIdType = partyOffsetContractIdType
  protected[this] def nameType: Fragment // Name in daml-lf-1.rst
  protected[this] def agreementTextType: Fragment

  // The max list size that can be used in `IN` clauses
  protected[this] def maxListSize: Option[Int]

  protected[this] def jsonColumn(name: Fragment): Fragment
  protected[this] def keyHashColumn: Fragment

  private[this] val createTemplateIdsTable = CreateTable(
    templateIdTableNameRaw,
    sql"""
      CREATE TABLE
        $templateIdTableName
        (tpid $bigSerialType NOT NULL CONSTRAINT ${tablePrefixFr}template_id_k PRIMARY KEY
        ,package_id $packageIdType NOT NULL
        ,template_module_name $nameType NOT NULL
        ,template_entity_name $nameType NOT NULL
        ,UNIQUE (package_id, template_module_name, template_entity_name)
        )
    """,
  )

  private[this] val createVersionTable = CreateTable(
    jsonApiSchemaVersionTableNameRaw,
    sql"""
       CREATE TABLE
        $jsonApiSchemaVersionTableName
        (version $bigIntType NOT NULL
        ,PRIMARY KEY (version)
        )
     """,
  )

  private[this] def initDatabaseDdls: Vector[InitDdl] =
    Vector(
      createTemplateIdsTable,
      createOffsetTable,
      createContractsTable,
    ) ++ extraDatabaseDdls :+ createVersionTable

  protected[this] def extraDatabaseDdls: Seq[InitDdl]

  private[http] final def dropAllTablesIfExist(implicit log: LogHandler): ConnectionIO[Unit] = {
    import cats.instances.vector._, cats.syntax.foldable.{toFoldableOps => ToFoldableOps}
    initDatabaseDdls.reverse
      .collect { case d: Droppable => dropIfExists(d) }
      .traverse_(_.update.run)
  }

  protected[this] def insertVersion(): ConnectionIO[Unit] =
    sql"""
         INSERT INTO $jsonApiSchemaVersionTableName (version)
         VALUES ($schemaVersion)
         """.update.run.map(_ => ())

  private[http] final def initDatabase(implicit log: LogHandler): ConnectionIO[Unit] = {
    import cats.instances.vector._, cats.syntax.foldable.{toFoldableOps => ToFoldableOps},
    cats.syntax.apply._
    initDatabaseDdls.traverse_(_.create.update.run) *> insertVersion()
  }

  protected[http] def version()(implicit log: LogHandler): ConnectionIO[Option[Int]]

  final def surrogateTemplateId(packageId: String, moduleName: String, entityName: String)(implicit
      log: LogHandler,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[SurrogateTpId] = {
    surrogateTpIdCache
      .getCacheValue(packageId, moduleName, entityName)
      .map { tpId =>
        Applicative[ConnectionIO].pure(tpId)
      }
      .getOrElse {
        surrogateTemplateIdFromDb(packageId, moduleName, entityName).map { tpId =>
          surrogateTpIdCache.setCacheValue(packageId, moduleName, entityName, tpId)
          tpId
        }
      }
  }

  private def surrogateTemplateIdFromDb(packageId: String, moduleName: String, entityName: String)(
      implicit log: LogHandler
  ): ConnectionIO[SurrogateTpId] =
    sql"""SELECT tpid FROM $templateIdTableName
          WHERE (package_id = $packageId AND template_module_name = $moduleName
                 AND template_entity_name = $entityName)"""
      .query[SurrogateTpId]
      .option flatMap {
      _.cata(
        _.pure[ConnectionIO],
        sql"""INSERT INTO $templateIdTableName (package_id, template_module_name, template_entity_name)
              VALUES ($packageId, $moduleName, $entityName)""".update
          .withUniqueGeneratedKeys[SurrogateTpId]("tpid"),
      )
    }

  final def lastOffset(parties: OneAnd[Set, String], tpid: SurrogateTpId)(implicit
      log: LogHandler
  ): ConnectionIO[Map[String, String]] = {
    import Queries.CompatImplicits.catsReducibleFromFoldable1
    val q = sql"""
      SELECT party, last_offset FROM $ledgerOffsetTableName WHERE tpid = $tpid AND
    """ ++ Fragments.in(fr"party", parties)
    q.query[(String, String)]
      .to[Vector]
      .map(_.toMap)
  }

  /** Template IDs, parties, and offsets that don't match expected offset values for
    * a particular `tpid`.
    */
  private[http] final def unsyncedOffsets(
      expectedOffset: String,
      tpids: NonEmpty[Set[SurrogateTpId]],
  ): ConnectionIO[Map[SurrogateTpId, Map[String, String]]] = {
    val condition = {
      import Queries.CompatImplicits.catsReducibleFromFoldable1, scalaz.std.iterable._
      Fragments.in(fr"tpid", tpids.toSeq.toOneAnd)
    }
    val q = sql"""
      SELECT tpid, party, last_offset FROM $ledgerOffsetTableName
             WHERE last_offset <> $expectedOffset AND $condition
    """
    q.query[(SurrogateTpId, String, String)]
      .map { case (tpid, party, offset) => (tpid, (party, offset)) }
      .to[Vector]
      .map(groupUnsyncedOffsets(tpids, _))
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
    val insert =
      Update[(String, SurrogateTpId, String)](
        s"""INSERT INTO ${tablePrefix}ledger_offset VALUES(?, ?, ?)"""
      )
    // If a concurrent transaction updated the offset for an existing party, we will get
    // fewer rows and throw a StaleOffsetException in the caller.
    val update = existingParties match {
      case hdP +: tlP =>
        Some(
          sql"""UPDATE $ledgerOffsetTableName SET last_offset = $newOffset
            WHERE ${Fragments.in(fr"party", cats.data.OneAnd(hdP, tlP))}
                  AND tpid = $tpid
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

  // different databases encode contract keys in different formats
  protected[this] type DBContractKey
  protected[this] def toDBContractKey[CK: JsonWriter](ck: CK): DBContractKey

  final def insertContracts[F[_]: cats.Foldable: Functor, CK: JsonWriter, PL: JsonWriter](
      dbcs: F[DBContract[SurrogateTpId, CK, PL, Seq[String]]]
  )(implicit log: LogHandler): ConnectionIO[Int] =
    primInsertContracts(dbcs.map(_.mapKeyPayloadParties(toDBContractKey(_), _.toJson, _.toArray)))

  protected[this] def primInsertContracts[F[_]: cats.Foldable: Functor](
      dbcs: F[DBContract[SurrogateTpId, DBContractKey, JsValue, Array[String]]]
  )(implicit log: LogHandler): ConnectionIO[Int]

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
        val chunks = maxListSize.fold(Vector(cids))(size => cids.grouped(size).toVector)
        chunks
          .map(chunk =>
            (fr"DELETE FROM $contractTableName WHERE " ++ Fragments
              .in(fr"contract_id", chunk)).update.run
          )
          .foldA
    }
  }

  private[http] final def selectContracts(
      parties: OneAnd[Set, String],
      tpid: SurrogateTpId,
      predicate: Fragment,
  )(implicit
      log: LogHandler
  ): Query0[DBContract[Unit, JsValue, JsValue, Vector[String]]] =
    selectContractsMultiTemplate(parties, ISeq((tpid, predicate)), MatchedQueryMarker.Unused)
      .map(_ copy (templateId = ()))

  /** Make a query that may indicate
    * which query or queries produced each contract.
    */
  private[http] def selectContractsMultiTemplate[Mark](
      parties: OneAnd[Set, String],
      queries: ISeq[(SurrogateTpId, Fragment)],
      trackMatchIndices: MatchedQueryMarker[Mark],
  )(implicit
      log: LogHandler
  ): Query0[DBContract[Mark, JsValue, JsValue, Vector[String]]]

  // implementation aid for selectContractsMultiTemplate
  @nowarn("msg=parameter value evidence.* is never used")
  protected[this] final def queryByCondition[Mark, Key: Read, SigsObs: Read, Agreement: Read](
      queries: ISeq[(SurrogateTpId, Fragment)],
      trackMatchIndices: MatchedQueryMarker[Mark],
      tpidSelector: Fragment,
      query: (Fragment, Fragment) => Fragment,
      key: Key => JsValue,
      sigsObs: SigsObs => Vector[String],
      agreement: Agreement => String,
  )(implicit
      log: LogHandler
  ): Query0[DBContract[Mark, JsValue, JsValue, Vector[String]]] = {
    val NonEmpty(queryConditions) = queries.toVector
    val queriesCondition =
      joinFragment(
        queryConditions.toOneAnd map { case (tpid, predicate) =>
          fr"($tpid = $tpidSelector AND ($predicate))"
        },
        fr" OR ",
      )
    // we effectively shadow Mark because Scala 2.12 doesn't quite get
    // that it should use the GADT type equality otherwise
    def goQuery[Mark0: Read](
        tpid: Fragment
    ): Query0[DBContract[Mark0, JsValue, JsValue, Vector[String]]] = {
      val q = query(tpid, queriesCondition)
      q.query[
        (String, Mark0, Key, Option[String], JsValue, SigsObs, SigsObs, Agreement)
      ].map { case (cid, tpid, rawKey, keyHash, payload, signatories, observers, rawAgreement) =>
        DBContract(
          contractId = cid,
          templateId = tpid,
          key = key(rawKey),
          keyHash = keyHash,
          payload = payload,
          signatories = sigsObs(signatories),
          observers = sigsObs(observers),
          agreementText = agreement(rawAgreement),
        )
      }
    }

    trackMatchIndices match {
      case MatchedQueryMarker.ByInt =>
        val tpid = projectedIndex(queries.zipWithIndex, tpidSelector = tpidSelector)
        goQuery[MatchedQueries](tpid)
      case MatchedQueryMarker.Unused =>
        goQuery[SurrogateTpId](tpidSelector)
    }
  }

  private[http] final def fetchById(
      parties: OneAnd[Set, String],
      tpid: SurrogateTpId,
      contractId: String,
  )(implicit
      log: LogHandler
  ): ConnectionIO[Option[DBContract[Unit, JsValue, JsValue, Vector[String]]]] =
    selectContracts(parties, tpid, sql"c.contract_id = $contractId").option

  private[http] final def fetchByKey(
      parties: OneAnd[Set, String],
      tpid: SurrogateTpId,
      key: Hash,
  )(implicit
      log: LogHandler
  ): ConnectionIO[Option[DBContract[Unit, JsValue, JsValue, Vector[String]]]] =
    selectContracts(parties, tpid, keyEquality(key)).option

  private[http] def keyEquality(key: Hash): Fragment = sql"key_hash = ${key.toHexString.toString}"

  private[http] def equalAtContractPath(path: JsonPath, literal: JsValue): Fragment

  private[http] def containsAtContractPath(path: JsonPath, literal: JsValue): Fragment

  private[http] def cmpContractPathToScalar(
      path: JsonPath,
      op: OrderOperator,
      literalScalar: JsValue,
  ): Fragment

  // unsure whether this will need to be instance dependent, but
  // just fine to be an alias for now
  private[http] val Implicits: Queries.Implicits.type = Queries.Implicits
}

object Queries {
  sealed trait SurrogateTpIdTag
  val SurrogateTpId = Tag.of[SurrogateTpIdTag]
  type SurrogateTpId = Long @@ SurrogateTpIdTag // matches tpid (BIGINT) above

  sealed trait MatchedQueriesTag
  val MatchedQueries = Tag.of[MatchedQueriesTag]
  type MatchedQueries = NonEmpty[ISeq[Int]] @@ MatchedQueriesTag

  // NB: #, order of arguments must match createContractsTable
  final case class DBContract[+TpId, +CK, +PL, +Prt](
      contractId: String,
      templateId: TpId,
      key: CK,
      keyHash: Option[String],
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
    // something that we must drop to re-create the DB
    sealed abstract class Droppable(val what: String) extends InitDdl {
      val name: String
    }
    final case class CreateTable(name: String, create: Fragment) extends Droppable("TABLE")
    final case class CreateMaterializedView(name: String, create: Fragment)
        extends Droppable("MATERIALIZED VIEW")
    final case class CreateIndex(create: Fragment) extends InitDdl
    final case class DoMagicSetup(create: Fragment) extends InitDdl
  }

  /** Whether selectContractsMultiTemplate computes a matchedQueries marker.
    *
    * @tparam Mark The "marker" indicating which queries matched.
    */
  private[http] sealed abstract class MatchedQueryMarker[Mark] extends Product with Serializable
  private[http] object MatchedQueryMarker {
    case object ByInt extends MatchedQueryMarker[MatchedQueries]
    case object Unused extends MatchedQueryMarker[SurrogateTpId]
  }

  /** Path to a location in a JSON tree. */
  private[http] final case class JsonPath(elems: Vector[Ref.Name \/ _0.type]) {
    def arrayAt(i: _0.type) = JsonPath(elems :+ \/-(i))
    def objectAt(k: Ref.Name) = JsonPath(elems :+ -\/(k))
  }

  private[http] sealed abstract class OrderOperator extends Product with Serializable
  private[http] object OrderOperator {
    case object LT extends OrderOperator
    case object LTEQ extends OrderOperator
    case object GT extends OrderOperator
    case object GTEQ extends OrderOperator
  }

  private[http] def joinFragment(xs: OneAnd[Vector, Fragment], sep: Fragment): Fragment =
    concatFragment(intersperse(xs, sep))

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

  private[this] def intersperse[A](oaa: OneAnd[Vector, A], a: A): OneAnd[Vector, A] =
    OneAnd(oaa.head, oaa.tail.flatMap(Vector(a, _)))

  private[this] def caseLookupFragment[SelEq: Put](
      m: Map[SelEq, Fragment],
      selector: Fragment,
  ): Fragment =
    fr"CASE" ++ {
      assert(m.nonEmpty, "existing offsets must be non-empty")
      val when +: whens = m.iterator.map { case (k, v) =>
        fr"WHEN ($selector = $k) THEN $v"
      }.toVector
      concatFragment(OneAnd(when, whens))
    } ++ fr"ELSE NULL END"

  private[dbbackend] def caseLookup[SelEq: Put, Then: Put](
      m: Map[SelEq, Then],
      selector: Fragment,
  ): Fragment =
    caseLookupFragment(m transform { (_, e) => fr"$e" }, selector)

  // an expression that yields a comma-terminated/separated list of SQL-side
  // string conversions of `Ix`es indicating which tpid/query pairs matched
  private[dbbackend] def projectedIndex[Ix: Put](
      queries: ISeq[((SurrogateTpId, Fragment), Ix)],
      tpidSelector: Fragment,
  ): Fragment = {
    import Implicits._
    caseLookupFragment(
      // SortedMap is only used so the tests are consistent; the SQL semantics
      // don't care what order this map is in
      SortedMap.from(queries.groupBy1(_._1._1)).transform {
        case (_, (_, ix) +-: ISeq()) => fr"${ix: Ix}||''"
        case (_, tqixes) =>
          concatFragment(
            intersperse(
              tqixes.toVector.toOneAnd.map { case ((_, q), ix) =>
                fr"(CASE WHEN ($q) THEN ${ix: Ix}||',' ELSE '' END)"
              },
              fr"||",
            )
          )
      },
      selector = tpidSelector,
    )
  }

  private[dbbackend] def groupUnsyncedOffsets[TpId, Party, Off](
      allTpids: Set[TpId],
      queryResult: Vector[(TpId, (Party, Off))],
  ): Map[TpId, Map[Party, Off]] = {
    val grouped = queryResult.groupBy1(_._1).transform((_, tpos) => tpos.view.map(_._2).toMap)
    // lagging offsets still needs to consider the template IDs that weren't
    // returned by the offset table query
    (allTpids diff grouped.keySet).view.map((_, Map.empty[Party, Off])).toMap ++ grouped
  }

  import doobie.util.invariant.InvalidValue

  @throws[InvalidValue[_, _]]
  private[this] def assertReadProjectedIndex(from: Option[String]): NonEmpty[ISeq[Int]] = {
    def invalid(reason: String) = {
      import cats.instances.option._, cats.instances.string._
      throw InvalidValue[Option[String], ISeq[Int]](from, reason = reason)
    }
    from.cata(
      { s =>
        val matches = s split ',' collect {
          case e if e.nonEmpty => e.parseInt.fold(err => invalid(err.getMessage), identity)
        }
        matches.to(ISeq)
      },
      ISeq.empty,
    ) match {
      case NonEmpty(matches) => matches
      case _ => invalid("matched row, but no matching index found; this indicates a query bug")
    }
  }

  private[http] val Postgres: QueryBackend.Aux[SqlInterpolation.StringArray] =
    PostgresQueryBackend
  private[http] val Oracle: QueryBackend.Aux[SqlInterpolation.Unused] =
    OracleQueryBackend

  private[http] object SqlInterpolation {
    final class StringArray()(implicit val gas: Get[Array[String]], val pas: Put[Array[String]])
    final class Unused()
  }

  private[http] object Implicits {
    implicit val `JsValue put`: Meta[JsValue] =
      Meta[String].timap(_.parseJson)(_.compactPrint)

    implicit val `SurrogateTpId meta`: Meta[SurrogateTpId] =
      SurrogateTpId subst Meta[Long]

    implicit val `SurrogateTpId ordering`: Ordering[SurrogateTpId] =
      SurrogateTpId subst implicitly[Ordering[Long]]

    implicit val `MatchedQueries get`: Read[MatchedQueries] =
      MatchedQueries subst (Read[Option[String]] map assertReadProjectedIndex)
  }

  private[dbbackend] object CompatImplicits {
    implicit def catsReducibleFromFoldable1[F[_]](implicit
        F: scalaz.Foldable1[F]
    ): cats.Reducible[F] = new cats.Reducible[F] {
      import cats.Eval

      override def foldLeft[A, B](fa: F[A], z: B)(f: (B, A) => B) = F.foldLeft(fa, z)(f)

      override def foldRight[A, B](fa: F[A], z: Eval[B])(f: (A, Eval[B]) => Eval[B]) =
        F.foldRight(fa, z)((a, eb) => f(a, Eval defer eb))

      override def reduceLeftTo[A, B](fa: F[A])(z: A => B)(f: (B, A) => B) =
        F.foldMapLeft1(fa)(z)(f)

      override def reduceRightTo[A, B](fa: F[A])(z: A => B)(f: (A, Eval[B]) => Eval[B]) =
        F.foldMapRight1(fa)(a => Eval later z(a))((a, eb) => f(a, Eval defer eb))
    }

    implicit def monadFromCatsMonad[F[_]](implicit F: cats.Monad[F]): scalaz.Monad[F] =
      new scalaz.Monad[F] {
        override def bind[A, B](fa: F[A])(f: A => F[B]): F[B] = F.flatMap(fa)(f)
        override def point[A](a: => A): F[A] = F.point(a)
      }
  }
}

private final class PostgresQueries(tablePrefix: String, tpIdCacheMaxEntries: Long)(implicit
    ipol: Queries.SqlInterpolation.StringArray,
    metrics: Metrics,
) extends Queries(tablePrefix, tpIdCacheMaxEntries) {
  import Queries._, Queries.InitDdl.{Droppable, CreateIndex}
  import Implicits._

  type Conf = Unit

  protected[this] override def dropIfExists(d: Droppable) =
    Fragment.const(s"DROP ${d.what} IF EXISTS ${d.name}")

  protected[this] override def bigIntType = sql"BIGINT"
  protected[this] override def bigSerialType = sql"BIGSERIAL"
  private[this] def textType: Fragment = sql"TEXT"
  protected[this] override def packageIdType = textType
  protected[this] override def partyOffsetContractIdType = textType
  protected[this] override def nameType = textType
  protected[this] override def agreementTextType = sql"TEXT NOT NULL"

  protected[this] override def jsonColumn(name: Fragment) = name ++ sql" JSONB NOT NULL"
  protected[this] override def keyHashColumn = textType

  protected[this] override val maxListSize = None

  protected[this] val contractsTableIndexName = Fragment.const0(s"${tablePrefix}contract_tpid_idx")

  private[this] val indexContractsTable = CreateIndex(sql"""
      CREATE INDEX $contractsTableIndexName ON $contractTableName (tpid)
    """)

  private[this] val contractKeyHashIndexName = Fragment.const0(s"${tablePrefix}ckey_hash_idx")
  private[this] val contractKeyHashIndex = CreateIndex(
    sql"""CREATE UNIQUE INDEX $contractKeyHashIndexName ON $contractTableName (key_hash)"""
  )

  protected[this] override def extraDatabaseDdls =
    Seq(indexContractsTable, contractKeyHashIndex)

  protected[http] override def version()(implicit log: LogHandler): ConnectionIO[Option[Int]] = {
    for {
      doesTableExist <-
        sql"""SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_name = '$jsonApiSchemaVersionTableName'
              )""".query[Boolean].unique
      version <-
        if (!doesTableExist) connection.pure(None)
        else sql"SELECT version FROM $jsonApiSchemaVersionTableName".query[Int].option
    } yield version
  }

  protected[this] override def contractsTableSignatoriesObservers = sql"""
    ,signatories TEXT ARRAY NOT NULL
    ,observers TEXT ARRAY NOT NULL
  """

  protected[this] type DBContractKey = JsValue

  protected[this] override def toDBContractKey[CK: JsonWriter](x: CK) = x.toJson

  protected[this] override def primInsertContracts[F[_]: cats.Foldable: Functor](
      dbcs: F[DBContract[SurrogateTpId, DBContractKey, JsValue, Array[String]]]
  )(implicit log: LogHandler): ConnectionIO[Int] = {
    import ipol.pas
    Update[DBContract[SurrogateTpId, JsValue, JsValue, Array[String]]](
      s"""
        INSERT INTO $contractTableNameRaw
        VALUES (?, ?, ?::jsonb, ?, ?::jsonb, ?, ?, ?)
        ON CONFLICT (contract_id) DO NOTHING
      """
    ).updateMany(dbcs)
  }

  private[http] override def selectContractsMultiTemplate[Mark](
      parties: OneAnd[Set, String],
      queries: ISeq[(SurrogateTpId, Fragment)],
      trackMatchIndices: MatchedQueryMarker[Mark],
  )(implicit
      log: LogHandler
  ): Query0[DBContract[Mark, JsValue, JsValue, Vector[String]]] = {
    val partyVector = parties.toVector
    import ipol.{gas, pas}
    queryByCondition(
      queries,
      trackMatchIndices,
      tpidSelector = fr"tpid",
      query = (tpid, unionPred) =>
        sql"""SELECT contract_id, $tpid tpid, key, key_hash, payload, signatories, observers, agreement_text
              FROM $contractTableName AS c
              WHERE (signatories && $partyVector::text[] OR observers && $partyVector::text[])
                    AND ($unionPred)""",
      key = identity[JsValue],
      sigsObs = identity[Vector[String]],
      agreement = identity[String],
    )
  }

  private[this] def fragmentContractPath(path: JsonPath) =
    concatFragment(
      OneAnd(
        contractColumnName,
        path.elems.map(_.fold(k => sql"->${k: String}", (_: _0.type) => sql"->0")),
      )
    )

  private[http] override def equalAtContractPath(path: JsonPath, literal: JsValue) =
    fragmentContractPath(path) ++ sql" = ${literal}::jsonb"

  private[http] override def containsAtContractPath(path: JsonPath, literal: JsValue) =
    fragmentContractPath(path) ++ sql" @> ${literal}::jsonb"

  private[http] override def cmpContractPathToScalar(
      path: JsonPath,
      op: OrderOperator,
      literalScalar: JsValue,
  ) = {
    import OrderOperator._
    val opc = op match {
      case LT => sql"<"
      case LTEQ => sql"<="
      case GT => sql">"
      case GTEQ => sql">="
    }
    sql"${fragmentContractPath(path)} $opc ${literalScalar}::jsonb"
  }
}

import OracleQueries.DisableContractPayloadIndexing

private final class OracleQueries(
    tablePrefix: String,
    disableContractPayloadIndexing: DisableContractPayloadIndexing,
    tpIdCacheMaxEntries: Long,
)(implicit metrics: Metrics)
    extends Queries(tablePrefix, tpIdCacheMaxEntries) {
  import Queries._, InitDdl._
  import Implicits._

  protected[this] override def dropIfExists(d: Droppable) = {
    val sqlCode = d match {
      case _: CreateTable => -942
      case _: CreateMaterializedView => -12003
    }
    sql"""BEGIN
      EXECUTE IMMEDIATE ${s"DROP ${d.what} ${d.name}"};
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE != $sqlCode THEN
          RAISE;
        END IF;
    END;"""
  }

  protected[this] override def bigIntType = sql"NUMBER(19,0)"
  protected[this] override def bigSerialType =
    sql"$bigIntType GENERATED ALWAYS AS IDENTITY"
  protected[this] override def packageIdType = sql"NVARCHAR2(64)"
  protected[this] override def partyOffsetContractIdType = sql"VARCHAR2(255)"
  // if >=1578: ORA-01450: maximum key length (6398) exceeded
  protected[this] override def nameType = sql"NVARCHAR2(1562)"
  protected[this] override def agreementTextType = sql"NCLOB"

  protected[this] override def jsonColumn(name: Fragment) =
    sql"$name CLOB NOT NULL CONSTRAINT ${tablePrefixFr}ensure_json_$name CHECK ($name IS JSON)"

  protected[this] override def keyHashColumn = sql"NVARCHAR2(64)"

  // See http://www.dba-oracle.com/t_ora_01795_maximum_number_of_expressions_in_a_list_is_1000.htm
  protected[this] override def maxListSize = Some(1000)

  protected[this] override def contractsTableSignatoriesObservers =
    sql"""
        ,${jsonColumn(sql"signatories")}
        ,${jsonColumn(sql"observers")}
        """
  private[this] val contractStakeholdersViewNameRaw = s"${tablePrefix}contract_stakeholders"
  private[this] val contractStakeholdersViewName =
    Fragment.const0(contractStakeholdersViewNameRaw)
  private[this] def stakeholdersView = CreateMaterializedView(
    contractStakeholdersViewNameRaw,
    sql"""CREATE MATERIALIZED VIEW $contractStakeholdersViewName
          BUILD IMMEDIATE REFRESH FAST ON STATEMENT AS
          SELECT contract_id, tpid, stakeholder FROM $contractTableName,
                 json_table(json_array(signatories, observers), '$$[*][*]'
                    columns (stakeholder $partyType path '$$'))""",
  )
  private[this] val stakeholdersIndexName = Fragment.const0(s"${tablePrefix}stakeholder_idx")
  private[this] def stakeholdersIndex = CreateIndex(
    sql"""CREATE INDEX $stakeholdersIndexName ON $contractStakeholdersViewName (tpid, stakeholder)"""
  )

  private[this] val contractKeyHashIndexName = Fragment.const0(s"${tablePrefix}ckey_hash_idx")
  private[this] val contractKeyHashIndex = CreateIndex(
    sql"""CREATE UNIQUE INDEX $contractKeyHashIndexName ON $contractTableName (key_hash)"""
  )

  private[this] val indexPayload = CreateIndex(sql"""
    CREATE SEARCH INDEX ${tablePrefixFr}payload_json_idx
    ON $contractTableName (payload) FOR JSON
    PARAMETERS('DATAGUIDE OFF')""")

  protected[this] override def extraDatabaseDdls =
    Seq(stakeholdersView, stakeholdersIndex, contractKeyHashIndex) ++
      (if (disableContractPayloadIndexing) Seq.empty else Seq(indexPayload))

  protected[http] override def version()(implicit log: LogHandler): ConnectionIO[Option[Int]] = {
    import cats.implicits._
    for {
      // Note that Oracle table names seem to be somewhat case sensitive,
      // but are inside the USER_TABLES table all uppercase.
      res <-
        sql"""SELECT 1 FROM USER_TABLES
              WHERE TABLE_NAME = UPPER('$jsonApiSchemaVersionTableName')"""
          .query[Int]
          .option
      version <-
        res.flatTraverse { _ =>
          sql"SELECT version FROM $jsonApiSchemaVersionTableName".query[Int].option
        }
    } yield version
  }

  protected[this] type DBContractKey = JsValue

  protected[this] override def toDBContractKey[CK: JsonWriter](x: CK): DBContractKey =
    JsObject(Map("key" -> x.toJson))

  protected[this] override def primInsertContracts[F[_]: cats.Foldable: Functor](
      dbcs: F[DBContract[SurrogateTpId, DBContractKey, JsValue, Array[String]]]
  )(implicit log: LogHandler): ConnectionIO[Int] = {
    import spray.json.DefaultJsonProtocol._
    Update[DBContract[SurrogateTpId, DBContractKey, JsValue, JsValue]](
      s"""
        INSERT /*+ ignore_row_on_dupkey_index($contractTableNameRaw(contract_id)) */
        INTO $contractTableNameRaw (contract_id, tpid, key, key_hash, payload, signatories, observers, agreement_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      """
    ).updateMany(
      dbcs.map(_.mapKeyPayloadParties(identity, identity, _.toJson))
    )
  }

  private[http] override def selectContractsMultiTemplate[Mark](
      parties: OneAnd[Set, String],
      queries: ISeq[(SurrogateTpId, Fragment)],
      trackMatchIndices: MatchedQueryMarker[Mark],
  )(implicit
      log: LogHandler
  ): Query0[DBContract[Mark, JsValue, JsValue, Vector[String]]] =
    queryByCondition(
      queries,
      trackMatchIndices,
      tpidSelector = fr"cst.tpid",
      query = { (tpid, queriesCondition) =>
        val rownum = parties.tail.nonEmpty option fr""",
            row_number() over (PARTITION BY c.contract_id ORDER BY c.contract_id) AS rownumber"""
        import Queries.CompatImplicits.catsReducibleFromFoldable1
        val outerSelectList =
          sql"""contract_id, template_id, key, key_hash, payload,
                signatories, observers, agreement_text"""
        val dupQ =
          sql"""SELECT c.contract_id contract_id, $tpid template_id, key, key_hash, payload,
                       signatories, observers, agreement_text ${rownum getOrElse fr""}
                FROM $contractTableName c
                     JOIN $contractStakeholdersViewName cst ON (c.contract_id = cst.contract_id)
                WHERE (${Fragments.in(fr"cst.stakeholder", parties)})
                      AND ($queriesCondition)"""
        rownum.fold(dupQ)(_ => sql"SELECT $outerSelectList FROM ($dupQ) WHERE rownumber = 1")
      },
      key = (_: JsValue).asJsObject.fields("key"),
      sigsObs = { (jsEncoded: JsValue) =>
        import spray.json.DefaultJsonProtocol._
        jsEncoded.convertTo[Vector[String]]
      },
      agreement = (_: Option[String]) getOrElse "",
    )

  private[this] def pathSteps(path: JsonPath): Cord =
    path.elems.foldMap(_.fold(k => (".\"": Cord) ++ k :- '"', (_: _0.type) => "[0]"))

  // I cannot believe this function exists in 2021
  // None if literal is too long
  // ORA-40454: path expression not a literal
  private[this] def oraclePathEscape(readyPath: Cord): Option[Fragment] = for {
    s <- Some(readyPath.toString)
    if s.size <= literalStringSizeLimit
    _ = assert(
      !s.startsWith("'") && !s.endsWith("'"),
      "Oracle JSON query syntax doesn't allow ' at beginning or ending",
    )
    escaped = s.replace("'", "''")
    if escaped.length <= literalStringSizeLimit
  } yield Fragment const0 ("'" + escaped + "'")
  // ORA-01704: string literal too long
  private[this] val literalStringSizeLimit = 4000

  private[this] def oracleShortPathEscape(readyPath: Cord): Fragment =
    oraclePathEscape(readyPath).getOrElse(
      throw new IllegalArgumentException(s"path too long: $readyPath")
    )

  private[http] override def equalAtContractPath(path: JsonPath, literal: JsValue): Fragment = {
    val opath: Cord = '$' -: pathSteps(path)
    // you cannot put a positional parameter in a path, which _must_ be a literal
    // so pass it as the path-local variable X instead
    def existsForm[Lit: Put](literal: Lit) =
      (
        "?(@ == $X)", // not a Scala interpolation
        sql" PASSING $literal AS X",
      )
    val predExtension = literal match {
      case JsNumber(n) => Some(existsForm(n))
      case JsString(s) => Some(existsForm(s))
      case JsTrue | JsFalse | JsNull => Some((s"?(@ == $literal)", sql""))
      case JsObject(_) | JsArray(_) => None
    }
    predExtension.cata(
      { case (pred, extension) =>
        sql"JSON_EXISTS($contractColumnName, " ++
          sql"${oracleShortPathEscape(opath ++ Cord(pred))}$extension)"
      },
      sql"JSON_EQUAL(JSON_QUERY($contractColumnName, " ++
        sql"${oracleShortPathEscape(opath)} RETURNING CLOB), $literal)",
    )
  }

  // XXX JsValue is _too big_ a type for `literal`; we can make this function
  // more obviously correct by using something that constructively eliminates
  // nonsense cases
  private[http] override def containsAtContractPath(path: JsonPath, literal: JsValue) = {
    def ensureNotNull = {
      // we are only trying to reject None for an Optional record/variant/list
      val pred: Cord = ('$' -: pathSteps(path)) ++ "?(@ != null)"
      sql"JSON_EXISTS($contractColumnName, ${oracleShortPathEscape(pred)})"
    }
    literal match {
      case JsTrue | JsFalse | JsNull | JsNumber(_) | JsString(_) =>
        equalAtContractPath(path, literal)

      case JsObject(fields) =>
        fields.toVector match {
          case hp +: tp =>
            // this assertFromString is forced by the aforementioned too-big type
            val fieldPreds = OneAnd(hp, tp).map { case (ok, ov) =>
              containsAtContractPath(path objectAt Ref.Name.assertFromString(ok), ov)
            }
            joinFragment(fieldPreds, sql" AND ")
          case _ =>
            // a check *at root* for `@> {}` always succeeds, so don't bother querying
            if (path.elems.isEmpty) sql"1 = 1"
            else ensureNotNull
        }

      case JsArray(Seq()) => ensureNotNull
      case JsArray(Seq(nestedOptInner)) =>
        containsAtContractPath(path arrayAt _0, nestedOptInner)
      // this case is forced by the aforementioned too-big type
      case JsArray(badElems) =>
        throw new IllegalStateException(
          s"multiple-element arrays should never be queried: $badElems"
        )
    }
  }

  // XXX as with containsAtContractPath, literalScalar is too big a type
  private[http] override def cmpContractPathToScalar(
      path: JsonPath,
      op: OrderOperator,
      literalScalar: JsValue,
  ) = {
    val literalRendered = literalScalar match {
      case JsNumber(n) => sql"$n"
      case JsString(s) => sql"$s"
      case JsNull | JsTrue | JsFalse | JsArray(_) | JsObject(_) =>
        throw new IllegalArgumentException(
          s"${literalScalar.compactPrint} is not comparable in JSON queries"
        )
    }
    import OrderOperator._
    val opc = op match {
      case LT => "<"
      case LTEQ => "<="
      case GT => ">"
      case GTEQ => ">="
    }
    val pathc = ('$' -: pathSteps(path)) ++ s"?(@ $opc ${"$X"})"
    sql"JSON_EXISTS($contractColumnName, " ++
      sql"${oracleShortPathEscape(pathc)} PASSING $literalRendered AS X)"
  }
}

private[http] object OracleQueries {
  val DisableContractPayloadIndexing = "disableContractPayloadIndexing"
  type DisableContractPayloadIndexing = Boolean
}
