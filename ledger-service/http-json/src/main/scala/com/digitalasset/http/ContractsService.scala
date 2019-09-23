// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.digitalasset.daml.lf.value.{Value => V}
import com.digitalasset.http.domain.{GetActiveContractsRequest, JwtPayload, TemplateId}
import com.digitalasset.http.query.ValuePredicate
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.IdentifierConverters.apiIdentifier
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.{-\/, Show, \/, \/-}
import spray.json.JsValue

import scala.concurrent.{ExecutionContext, Future}

class ContractsService(
    resolveTemplateIds: PackageService.ResolveTemplateIds,
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    lookupType: query.ValuePredicate.TypeLookup,
    parallelism: Int = 8)(implicit ec: ExecutionContext, mat: Materializer) {

  import ContractsService._

  type Result = (Seq[ActiveContract], CompiledPredicates)
  type CompiledPredicates = Map[domain.TemplateId.RequiredPkg, query.ValuePredicate]

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
      contractKey: lav1.value.Value): Future[Option[ActiveContract]] =
    for {
      (as, _) <- search(jwt, party, Set(templateId), Map.empty)
      a = findByContractKey(contractKey)(as)
    } yield a

  private def findByContractKey(k: lav1.value.Value)(
      as: Seq[ActiveContract]): Option[domain.ActiveContract[lav1.value.Value]] =
    as.view.find(isContractKey(k))

  private def isContractKey(k: lav1.value.Value)(
      a: domain.ActiveContract[lav1.value.Value]): Boolean =
    a.key.fold(false)(_ == k)

  def lookup(
      jwt: Jwt,
      party: lar.Party,
      templateId: Option[TemplateId.OptionalPkg],
      contractId: String): Future[Option[ActiveContract]] =
    for {
      (as, _) <- search(jwt, party, templateIds(templateId), Map.empty)
      a = findByContractId(contractId)(as)
    } yield a

  private def templateIds(a: Option[TemplateId.OptionalPkg]): Set[TemplateId.OptionalPkg] =
    a.toList.toSet

  private def findByContractId(k: String)(as: Seq[ActiveContract]): Option[ActiveContract] =
    as.find(x => (x.contractId: String) == k)

  def search(jwt: Jwt, jwtPayload: JwtPayload, request: GetActiveContractsRequest): Future[Result] =
    search(jwt, jwtPayload.party, request.templateIds, request.query)

  def search(
      jwt: Jwt,
      party: lar.Party,
      templateIds: Set[domain.TemplateId.OptionalPkg],
      queryParams: Map[String, JsValue]): Future[Result] =
    for {
      templateIds <- toFuture(resolveTemplateIds(templateIds))
      allActiveContracts <- getActiveContracts(jwt, transactionFilter(party, templateIds), true)
        .mapAsyncUnordered(parallelism)(gacr => toFuture(activeContracts(gacr)))
        .runWith(Sink.seq)
        .map(_.flatten): Future[Seq[ActiveContract]]
      predicates = templateIds.iterator.map(a => (a, valuePredicate(a, queryParams))).toMap
    } yield (allActiveContracts, predicates)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def activeContracts(gacr: lav1.active_contracts_service.GetActiveContractsResponse)
    : Error \/ List[ActiveContract] = {

    val workflowId = domain.WorkflowId.fromLedgerApi(gacr)

    val toAc: lav1.event.CreatedEvent => domain.Error \/ ActiveContract =
      domain.ActiveContract.fromLedgerApi(workflowId)

    gacr.activeContracts.toList
      .traverse(toAc)
      .leftMap(e => Error('activeContracts, e.shows))
  }

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
