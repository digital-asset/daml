// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.stream.scaladsl._
import akka.stream.{Materializer, SourceShape}
import com.digitalasset.daml.lf
import com.digitalasset.http.ContractsFetch.{InsertDeleteStep, OffsetBookmark}
import com.digitalasset.http.dbbackend.ContractDao
import com.digitalasset.http.domain.{GetActiveContractsRequest, JwtPayload, TemplateId}
import com.digitalasset.http.json.JsonProtocol.LfValueCodec
import com.digitalasset.http.query.ValuePredicate
import com.digitalasset.http.query.ValuePredicate.LfV
import com.digitalasset.http.util.ApiValueToLfValueConverter
import com.digitalasset.http.util.ExceptionOps._
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.IdentifierConverters.apiIdentifier
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => api}
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, Show, \/, \/-}
import spray.json.JsValue

import scala.concurrent.{ExecutionContext, Future}

// TODO(Leo) split it into ContractsServiceInMemory and ContractsServiceDb
class ContractsService(
    resolveTemplateIds: PackageService.ResolveTemplateIds,
    resolveTemplateId: PackageService.ResolveTemplateId,
    allTemplateIds: PackageService.AllTemplateIds,
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    lookupType: query.ValuePredicate.TypeLookup,
    contractDao: Option[dbbackend.ContractDao],
    parallelism: Int = 8)(implicit ec: ExecutionContext, mat: Materializer)
    extends StrictLogging {

  import ContractsService._

  type CompiledPredicates = Map[domain.TemplateId.RequiredPkg, query.ValuePredicate]

  private val daoAndFetch: Option[(dbbackend.ContractDao, ContractsFetch)] = contractDao.map {
    dao =>
      (
        dao,
        new ContractsFetch(getActiveContracts, getCreatesAndArchivesSince, lookupType)(
          dao.logHandler)
      )
  }

  def lookup(jwt: Jwt, jwtPayload: JwtPayload, contractLocator: domain.ContractLocator[ApiValue])
    : Future[Option[domain.ActiveContract[LfValue]]] =
    contractLocator match {
      case domain.EnrichedContractKey(templateId, contractKey) =>
        findByContractKey(jwt, jwtPayload.party, templateId, contractKey)
      case domain.EnrichedContractId(templateId, contractId) =>
        findByContractId(jwt, jwtPayload.party, templateId, contractId)
    }

  def findByContractKey(
      jwt: Jwt,
      party: lar.Party,
      templateId: TemplateId.OptionalPkg,
      contractKey: api.value.Value): Future[Option[domain.ActiveContract[LfValue]]] =
    for {

      lfKey <- toFuture(apiValueToLfValue(contractKey)): Future[LfValue]

      resolvedTemplateId <- toFuture(resolveTemplateId(templateId)): Future[TemplateId.RequiredPkg]

      predicate = isContractKey(keyToTuple(lfKey)) _

      errorOrAc <- searchInMemoryOneTpId(jwt, party, resolvedTemplateId, Map.empty)
        .collect {
          case e @ -\/(_) => e
          case a @ \/-(ac) if predicate(ac) => a
        }
        .runWith(Sink.headOption): Future[Option[Error \/ domain.ActiveContract[LfValue]]]

      result <- lookupResult(errorOrAc)

    } yield result

  private def isContractKey(k: LfValue)(a: domain.ActiveContract[LfValue]): Boolean =
    a.key.fold(false)(key => keyToTuple(key) == k)

  private def keyToTuple(a: LfValue): LfValue = a match {
    case lf.value.Value.ValueRecord(_, fields) =>
      lf.value.Value.ValueRecord(None, fields.map(k => (None, k._2)))
    case _ =>
      a
  }

  def findByContractId(
      jwt: Jwt,
      party: lar.Party,
      templateId: Option[domain.TemplateId.OptionalPkg],
      contractId: domain.ContractId): Future[Option[domain.ActiveContract[LfValue]]] =
    for {

      resolvedTemplateIds <- templateId.cata(
        x => toFuture(resolveTemplateId(x).map(Set(_))),
        Future.successful(allTemplateIds())
      ): Future[Set[domain.TemplateId.RequiredPkg]]

      errorOrAc <- searchInMemory(jwt, party, resolvedTemplateIds, Map.empty)
        .collect {
          case e @ -\/(_) => e
          case a @ \/-(ac) if isContractId(contractId)(ac) => a
        }
        .runWith(Sink.headOption): Future[Option[Error \/ domain.ActiveContract[LfValue]]]

      result <- lookupResult(errorOrAc)

    } yield result

  private def lookupResult(errorOrAc: Option[Error \/ domain.ActiveContract[LfValue]])
    : Future[Option[domain.ActiveContract[LfValue]]] = {
    errorOrAc.cata(x => toFuture(x).map(Some(_)), Future.successful(None))
  }

  private def isContractId(k: domain.ContractId)(a: domain.ActiveContract[LfValue]): Boolean =
    (a.contractId: domain.ContractId) == k

  def retrieveAll(
      jwt: Jwt,
      jwtPayload: JwtPayload): Source[Error \/ domain.ActiveContract[LfValue], NotUsed] =
    retrieveAll(jwt, jwtPayload.party)

  def retrieveAll(
      jwt: Jwt,
      party: domain.Party): Source[Error \/ domain.ActiveContract[LfValue], NotUsed] =
    Source(allTemplateIds()).flatMapConcat(x => searchInMemoryOneTpId(jwt, party, x, Map.empty))

  def search(jwt: Jwt, jwtPayload: JwtPayload, request: GetActiveContractsRequest)
    : Source[Error \/ domain.ActiveContract[JsValue], NotUsed] =
    search(jwt, jwtPayload.party, request.templateIds, request.query)

  def search(
      jwt: Jwt,
      party: domain.Party,
      templateIds: Set[domain.TemplateId.OptionalPkg],
      queryParams: Map[String, JsValue])
    : Source[Error \/ domain.ActiveContract[JsValue], NotUsed] = {

    resolveTemplateIds(templateIds) match {
      case -\/(e) =>
        Source.single(-\/(Error('search, e.shows)))
      case \/-(resolvedTemplateIds) =>
        daoAndFetch.cata(
          x => searchDb(x._1, x._2)(jwt, party, resolvedTemplateIds.toSet, queryParams),
          searchInMemory(jwt, party, resolvedTemplateIds.toSet, queryParams)
            .map(_.flatMap(lfAcToJsAc))
        )
    }
  }

  // we store create arguments as JASON in DB, that is why it is `domain.ActiveContract[JsValue]` in the result
  def searchDb(dao: dbbackend.ContractDao, fetch: ContractsFetch)(
      jwt: Jwt,
      party: domain.Party,
      templateIds: Set[domain.TemplateId.RequiredPkg],
      queryParams: Map[String, JsValue])
    : Source[Error \/ domain.ActiveContract[JsValue], NotUsed] = {

    // TODO use `stream` when materializing DBContracts, so we could stream ActiveContracts
    val fv: Future[Vector[domain.ActiveContract[JsValue]]] = dao
      .transact { searchDb_(fetch, dao.logHandler)(jwt, party, templateIds, queryParams) }
      .unsafeToFuture()

    Source.future(fv).mapConcat(identity).map(\/.right)
  }

  private def searchDb_(fetch: ContractsFetch, doobieLog: doobie.LogHandler)(
      jwt: Jwt,
      party: domain.Party,
      templateIds: Set[domain.TemplateId.RequiredPkg],
      queryParams: Map[String, JsValue])
    : doobie.ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
    import cats.instances.vector._
    import cats.syntax.traverse._
    import doobie.implicits._
    templateIds.toVector
      .traverse(tpId => searchDbOneTpId_(fetch, doobieLog)(jwt, party, tpId, queryParams))
      .map(_.flatten)
  }

  private def searchDbOneTpId_(fetch: ContractsFetch, doobieLog: doobie.LogHandler)(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg,
      queryParams: Map[String, JsValue])
    : doobie.ConnectionIO[Vector[domain.ActiveContract[JsValue]]] =
    for {
      _ <- fetch.fetchAndPersist(jwt, party, templateId)
      predicate = valuePredicate(templateId, queryParams)
      acs <- ContractDao.selectContracts(party, templateId, predicate.toSqlWhereClause)(doobieLog)
    } yield acs

  def searchInMemory(
      jwt: Jwt,
      party: domain.Party,
      templateIds: Set[domain.TemplateId.RequiredPkg],
      queryParams: Map[String, JsValue]): Source[Error \/ domain.ActiveContract[LfValue], NotUsed] =
    Source(templateIds)
      .flatMapConcat(x => searchInMemoryOneTpId(jwt, party, x, queryParams))

  private def searchInMemoryOneTpId(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg,
      queryParams: Map[String, JsValue])
    : Source[Error \/ domain.ActiveContract[LfValue], NotUsed] = {

    val empty = InsertDeleteStep[api.event.CreatedEvent](Vector.empty, Set.empty)
    def cid(a: api.event.CreatedEvent): String = a.contractId
    def append(
        a: InsertDeleteStep[api.event.CreatedEvent],
        b: InsertDeleteStep[api.event.CreatedEvent]) = a.appendWithCid(b)(cid)

    val predicate: ValuePredicate = valuePredicate(templateId, queryParams)
    val funPredicate: LfV => Boolean = predicate.toFunPredicate

    insertDeleteStepSource(jwt, party, templateId)
      .fold(empty)(append)
      .mapConcat(_.inserts)
      .map { apiEvent =>
        domain.ActiveContract
          .fromLedgerApi(apiEvent)
          .leftMap(e => Error('searchInMemory, e.shows))
          .flatMap(apiAcToLfAc): Error \/ domain.ActiveContract[LfValue]
      }
      .collect(collectActiveContracts(funPredicate))
  }

  private def insertDeleteStepSource(
      jwt: Jwt,
      party: lar.Party,
      templateId: domain.TemplateId.RequiredPkg)
    : Source[InsertDeleteStep[api.event.CreatedEvent], NotUsed] = {

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val source = getActiveContracts(jwt, transactionFilter(party, templateId), true)

      val transactionsSince
        : api.ledger_offset.LedgerOffset => Source[api.transaction.Transaction, NotUsed] =
        getCreatesAndArchivesSince(
          jwt,
          transactionFilter(party, templateId),
          _: api.ledger_offset.LedgerOffset)

      val contractsAndBoundary = b add ContractsFetch.acsFollowingAndBoundary(transactionsSince)
      val offsetSink = b add Sink.foreach[OffsetBookmark[String]] { a =>
        logger.debug(s"contracts fetch completed at: ${a.toString}")
      }

      source ~> contractsAndBoundary.in
      contractsAndBoundary.out1 ~> offsetSink
      new SourceShape(contractsAndBoundary.out0)
    }

    Source.fromGraph(graph)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def apiAcToLfAc(
      ac: domain.ActiveContract[ApiValue]): Error \/ domain.ActiveContract[LfValue] =
    ac.traverse(ApiValueToLfValueConverter.apiValueToLfValue)
      .leftMap(e => Error('apiAcToLfAc, e.shows))

  private def apiValueToLfValue(a: ApiValue): Error \/ LfValue =
    ApiValueToLfValueConverter.apiValueToLfValue(a).leftMap(e => Error('apiValueToLfValue, e.shows))

  private def collectActiveContracts(predicate: LfValue => Boolean): PartialFunction[
    Error \/ domain.ActiveContract[LfValue],
    Error \/ domain.ActiveContract[LfValue]
  ] = {
    case e @ -\/(_) => e
    case a @ \/-(ac) if predicate(ac.argument) => a
  }

  private def valuePredicate(
      templateId: domain.TemplateId.RequiredPkg,
      q: Map[String, JsValue]): query.ValuePredicate =
    ValuePredicate.fromTemplateJsObject(q, templateId, lookupType)

  private def transactionFilter(
      party: lar.Party,
      templateId: TemplateId.RequiredPkg): api.transaction_filter.TransactionFilter = {
    import api.transaction_filter._

    val filters = Filters(
      Some(api.transaction_filter.InclusiveFilters(List(apiIdentifier(templateId)))))

    TransactionFilter(Map(lar.Party.unwrap(party) -> filters))
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def lfAcToJsAc(
      a: domain.ActiveContract[LfValue]): Error \/ domain.ActiveContract[JsValue] =
    a.traverse(lfValueToJsValue)

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.fromTryCatchNonFatal(LfValueCodec.apiValueToJsValue(a)).leftMap(e =>
      Error('lfValueToJsValue, e.description))
}

object ContractsService {
  private type ApiValue = api.value.Value

  private type LfValue = lf.value.Value[lf.value.Value.AbsoluteContractId]

  case class Error(id: Symbol, message: String)

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"ContractService Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }
}
