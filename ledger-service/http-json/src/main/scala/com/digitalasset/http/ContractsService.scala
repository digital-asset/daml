// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.stream.scaladsl._
import akka.stream.{Materializer, SourceShape}
import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.value.{Value => V}
import com.digitalasset.http.ContractsFetch.{InsertDeleteStep, OffsetBookmark}
import com.digitalasset.http.domain.{GetActiveContractsRequest, JwtPayload, TemplateId}
import com.digitalasset.http.query.ValuePredicate
import com.digitalasset.http.query.ValuePredicate.LfV
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.IdentifierConverters.apiIdentifier
import com.digitalasset.http.util.{ApiValueToLfValueConverter, FutureUtil}
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

class ContractsService(
    resolveTemplateIds: PackageService.ResolveTemplateIds,
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    lookupType: query.ValuePredicate.TypeLookup,
    contractDao: Option[dbbackend.ContractDao],
    parallelism: Int = 8)(implicit ec: ExecutionContext, mat: Materializer)
    extends StrictLogging {

  import ContractsService._

  type CompiledPredicates = Map[domain.TemplateId.RequiredPkg, query.ValuePredicate]

  private val contractsFetch = contractDao.map { dao =>
    new ContractsFetch(getActiveContracts, getCreatesAndArchivesSince, lookupType)(dao.logHandler)
  }

  def lookup(jwt: Jwt, jwtPayload: JwtPayload, request: domain.ContractLookupRequest[ApiValue])
    : Future[Option[domain.ActiveContract[LfValue]]] =
    request.id match {
      case -\/((templateId, contractKey)) =>
        lookup(jwt, jwtPayload.party, templateId, contractKey)
      case \/-((templateId, contractId)) =>
        lookup(jwt, jwtPayload.party, templateId, contractId)
    }

  def lookup(
      jwt: Jwt,
      party: lar.Party,
      templateId: TemplateId.OptionalPkg,
      contractKey: api.value.Value): Future[Option[domain.ActiveContract[LfValue]]] =
    for {

      lfKey <- FutureUtil.toFuture(apiValueToLfValue(contractKey)): Future[LfValue]

      errorOrAc <- search(jwt, party, Set(templateId), Map.empty)
        .collect {
          case e @ -\/(_) => e
          case a @ \/-(ac) if isContractKey(lfKey)(ac) => a
        }
        .runWith(Sink.headOption): Future[Option[Error \/ domain.ActiveContract[LfValue]]]

      result <- lookupResult(errorOrAc)

    } yield result

  private def isContractKey(k: LfValue)(a: domain.ActiveContract[LfValue]): Boolean =
    a.key.fold(false)(_ == k)

  def lookup(
      jwt: Jwt,
      party: lar.Party,
      templateId: Option[TemplateId.OptionalPkg],
      contractId: domain.ContractId): Future[Option[domain.ActiveContract[LfValue]]] =
    for {

      errorOrAc <- search(jwt, party, templateIds(templateId), Map.empty)
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

  private def templateIds(a: Option[TemplateId.OptionalPkg]): Set[TemplateId.OptionalPkg] =
    a.toList.toSet

  private def isContractId(k: domain.ContractId)(a: domain.ActiveContract[LfValue]): Boolean =
    (a.contractId: domain.ContractId) == k

  def search(jwt: Jwt, jwtPayload: JwtPayload, request: GetActiveContractsRequest)
    : Source[Error \/ domain.ActiveContract[LfValue], NotUsed] =
    search(jwt, jwtPayload.party, request.templateIds, request.query)

  def search(
      jwt: Jwt,
      party: lar.Party,
      templateIds: Set[domain.TemplateId.OptionalPkg],
      queryParams: Map[String, JsValue])
    : Source[Error \/ domain.ActiveContract[LfValue], NotUsed] = {

    resolveTemplateIds(templateIds) match {
      case -\/(e) => Source.single(-\/(Error('search, e.shows)))
      case \/-(resolvedTemplateIds) =>
        // TODO(Leo/Stephen): fetchAndPersistContracts should be removed once we have SQL search ready
        val persistF: Future[Option[Unit]] =
          fetchAndPersistContracts(jwt, party, resolvedTemplateIds)
        // return source after we fetch and persist
        val resultSourceF = persistF.map { _ =>
          Source(resolvedTemplateIds)
            .flatMapConcat(tpId => searchInMemory(jwt, party, tpId, queryParams))
        }

        Source.fromFutureSource(resultSourceF).mapMaterializedValue(_ => NotUsed)
    }
  }

  private def searchInMemory(
      jwt: Jwt,
      party: lar.Party,
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

      val source = getActiveContracts(jwt, transactionFilter(party, List(templateId)), true)

      val transactionsSince
        : api.ledger_offset.LedgerOffset => Source[api.transaction.Transaction, NotUsed] =
        getCreatesAndArchivesSince(
          jwt,
          transactionFilter(party, List(templateId)),
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

  private def fetchAndPersistContracts(
      jwt: Jwt,
      party: lar.Party,
      templateIds: List[domain.TemplateId.RequiredPkg]): Future[Option[Unit]] = {

    import scalaz.std.option._
    import scalaz.std.scalaFuture._
    import scalaz.syntax.applicative._
    import scalaz.syntax.traverse._

    val option: Option[Future[Unit]] = ^(contractDao, contractsFetch)((dao, fetch) =>
      fetchAndPersistContracts(dao, fetch)(jwt, party, templateIds))

    option.sequence
  }

  private def fetchAndPersistContracts(dao: dbbackend.ContractDao, fetch: ContractsFetch)(
      jwt: Jwt,
      party: domain.Party,
      templateIds: List[domain.TemplateId.RequiredPkg]): Future[Unit] =
    dao.transact(fetch.contractsIo2(jwt, party, templateIds)).unsafeToFuture().map(_ => ())

  def filterSearch(
      compiledPredicates: CompiledPredicates,
      activeContracts: Seq[domain.ActiveContract[V[V.AbsoluteContractId]]])
    : Seq[domain.ActiveContract[V[V.AbsoluteContractId]]] = {
    val predFuns = compiledPredicates transform ((_, vp) => vp.toFunPredicate)
    activeContracts.filter(ac => predFuns get ac.templateId forall (_(ac.argument)))
  }

  private def valuePredicate(
      templateId: domain.TemplateId.RequiredPkg,
      q: Map[String, JsValue]): query.ValuePredicate =
    ValuePredicate.fromTemplateJsObject(q, templateId, lookupType)

  private def transactionFilter(
      party: lar.Party,
      templateIds: List[TemplateId.RequiredPkg]): api.transaction_filter.TransactionFilter = {
    import api.transaction_filter._

    val filters =
      if (templateIds.isEmpty) Filters.defaultInstance
      else Filters(Some(api.transaction_filter.InclusiveFilters(templateIds.map(apiIdentifier))))

    TransactionFilter(Map(lar.Party.unwrap(party) -> filters))
  }
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
