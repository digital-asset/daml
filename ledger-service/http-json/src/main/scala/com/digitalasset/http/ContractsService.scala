// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.stream.{Materializer, SourceShape}
import akka.stream.scaladsl._
import com.digitalasset.daml.lf.value.{Value => V}
import com.digitalasset.http.ContractsFetch.{InsertDeleteStep, OffsetBookmark}
import com.digitalasset.http.domain.TemplateId.RequiredPkg
import com.digitalasset.http.domain.{GetActiveContractsRequest, JwtPayload, TemplateId}
import com.digitalasset.http.query.ValuePredicate
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.IdentifierConverters.apiIdentifier
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.show._
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

  def lookup(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      request: domain.ContractLookupRequest[lav1.value.Value])
    : Future[Option[domain.ActiveContract[lav1.value.Value]]] =
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
      contractKey: lav1.value.Value): Future[Option[ActiveContract]] = {

    toFuture(search(jwt, party, Set(templateId), Map.empty)).flatMap {
      case (source, _) =>
        source
          .filter(isContractKey(contractKey))
          .runWith(Sink.headOption): Future[Option[ActiveContract]]
    }
  }

  private def isContractKey(k: lav1.value.Value)(
      a: domain.ActiveContract[lav1.value.Value]): Boolean =
    a.key.fold(false)(_ == k)

  def lookup(
      jwt: Jwt,
      party: lar.Party,
      templateId: Option[TemplateId.OptionalPkg],
      contractId: domain.ContractId): Future[Option[ActiveContract]] = {

    toFuture(search(jwt, party, templateIds(templateId), Map.empty)).flatMap {
      case (source, _) =>
        source
          .filter(isContractId(contractId))
          .runWith(Sink.headOption): Future[Option[ActiveContract]]
    }
  }

  private def templateIds(a: Option[TemplateId.OptionalPkg]): Set[TemplateId.OptionalPkg] =
    a.toList.toSet

  private def isContractId(k: domain.ContractId)(a: ActiveContract): Boolean =
    (a.contractId: domain.ContractId) == k

  def search(jwt: Jwt, jwtPayload: JwtPayload, request: GetActiveContractsRequest)
    : Error \/ (Source[ActiveContract, NotUsed], CompiledPredicates) =
    search(jwt, jwtPayload.party, request.templateIds, request.query)

  def search(
      jwt: Jwt,
      party: lar.Party,
      templateIds: Set[domain.TemplateId.OptionalPkg],
      queryParams: Map[String, JsValue])
    : Error \/ (Source[ActiveContract, NotUsed], CompiledPredicates) =
    for {
      resolvedTemplateIds <- resolveTemplateIds(templateIds)
        .leftMap(e => Error('search, e.shows)): Error \/ List[RequiredPkg]

      predicates = resolvedTemplateIds
        .map(x => (x, valuePredicate(x, queryParams)))
        .toMap: CompiledPredicates

      sourceF = fetchAndPersistContracts(jwt, party, resolvedTemplateIds).map { _ =>
        getActiveContracts(jwt, transactionFilter(party, resolvedTemplateIds), true)
          .mapAsyncUnordered(parallelism)(gacr => toFuture(activeContracts(gacr)))
          .mapConcat(identity): Source[ActiveContract, NotUsed]
      }: Future[Source[ActiveContract, NotUsed]]

      source = Source
        .fromFutureSource(sourceF)
        .mapMaterializedValue(_ => NotUsed): Source[ActiveContract, NotUsed]

    } yield (source, predicates)

  def searchInMemory(
      jwt: Jwt,
      party: lar.Party,
      templateId: domain.TemplateId.RequiredPkg,
      queryParams: Map[String, JsValue])
    : (Source[Error \/ ActiveContract, NotUsed], CompiledPredicates) = {

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val source = getActiveContracts(jwt, transactionFilter(party, List(templateId)), true)

      val transactionsSince
        : lav1.ledger_offset.LedgerOffset => Source[lav1.transaction.Transaction, NotUsed] =
        getCreatesAndArchivesSince(
          jwt,
          transactionFilter(party, List(templateId)),
          _: lav1.ledger_offset.LedgerOffset)

      val contractsAndBoundary = b add ContractsFetch.acsFollowingAndBoundary(transactionsSince)
      val offsetSink = b add Sink.foreach[OffsetBookmark[String]] { a =>
        logger.debug(s"search completed at: ${a.toString}")
      }

      source ~> contractsAndBoundary.in
      contractsAndBoundary.out1 ~> offsetSink
      new SourceShape(contractsAndBoundary.out0)
    }

    val empty = InsertDeleteStep[lav1.event.CreatedEvent](Vector.empty, Set.empty)
    def cid(a: lav1.event.CreatedEvent): String = a.contractId
    def append(
        a: InsertDeleteStep[lav1.event.CreatedEvent],
        b: InsertDeleteStep[lav1.event.CreatedEvent]) = a.appendWithCid(b)(cid)

    val source = Source
      .fromGraph(graph)
      .fold(empty)(append)
      .mapConcat(_.inserts)
      .map { x =>
        domain.ActiveContract.fromLedgerApi(x).leftMap(e => Error('searchInMemory, e.shows))
      }
    // TODO(Leo) build CompiledPredicates
    (source, Map.empty)
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

  private def activeContracts(gacr: lav1.active_contracts_service.GetActiveContractsResponse)
    : Error \/ List[ActiveContract] =
    domain.ActiveContract.fromLedgerApi(gacr).leftMap(e => Error('activeContracts, e.shows))

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
      templateIds: List[TemplateId.RequiredPkg]): lav1.transaction_filter.TransactionFilter = {
    import lav1.transaction_filter._

    val filters =
      if (templateIds.isEmpty) Filters.defaultInstance
      else Filters(Some(lav1.transaction_filter.InclusiveFilters(templateIds.map(apiIdentifier))))

    TransactionFilter(Map(lar.Party.unwrap(party) -> filters))
  }
}

object ContractsService {
  type ActiveContract = domain.ActiveContract[lav1.value.Value]

  case class Error(id: Symbol, message: String)

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"ContractService Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }
}
