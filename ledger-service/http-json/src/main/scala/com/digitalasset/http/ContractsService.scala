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
import scalaz.std.string._
import scalaz.{-\/, \/-}
import spray.json.JsValue

import scala.concurrent.{ExecutionContext, Future}

class ContractsService(
    resolveTemplateIds: PackageService.ResolveTemplateIds,
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    lookupType: query.ValuePredicate.TypeLookup,
    parallelism: Int = 8)(implicit ec: ExecutionContext, mat: Materializer) {

  type Result = (Seq[domain.GetActiveContractsResponse[lav1.value.Value]], CompiledPredicates)
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
      contractKey: lav1.value.Value): Future[Option[domain.ActiveContract[lav1.value.Value]]] =
    for {
      (as, _) <- search(jwt, party, Set(templateId), Map.empty)
      a = findByContractKey(contractKey)(as)
    } yield a

  private def findByContractKey(k: lav1.value.Value)(
      as: Seq[domain.GetActiveContractsResponse[lav1.value.Value]])
    : Option[domain.ActiveContract[lav1.value.Value]] =
    (as.view: Seq[domain.GetActiveContractsResponse[lav1.value.Value]])
      .flatMap(a => a.activeContracts)
      .find(isContractKey(k))

  private def isContractKey(k: lav1.value.Value)(
      a: domain.ActiveContract[lav1.value.Value]): Boolean =
    a.key.fold(false)(_ == k)

  def lookup(
      jwt: Jwt,
      party: lar.Party,
      templateId: Option[TemplateId.OptionalPkg],
      contractId: String): Future[Option[domain.ActiveContract[lav1.value.Value]]] =
    for {
      (as, _) <- search(jwt, party, templateIds(templateId), Map.empty)
      a = findByContractId(contractId)(as)
    } yield a

  private def templateIds(a: Option[TemplateId.OptionalPkg]): Set[TemplateId.OptionalPkg] =
    a.toList.toSet

  private def findByContractId(k: String)(
      as: Seq[domain.GetActiveContractsResponse[lav1.value.Value]])
    : Option[domain.ActiveContract[lav1.value.Value]] =
    (as.view: Seq[domain.GetActiveContractsResponse[lav1.value.Value]])
      .flatMap(a => a.activeContracts)
      .find(x => (x.contractId: String) == k)

  def search(jwt: Jwt, jwtPayload: JwtPayload, request: GetActiveContractsRequest): Future[Result] =
    search(jwt, jwtPayload.party, request.templateIds, request.query)

  def search(
      jwt: Jwt,
      party: lar.Party,
      templateIds: Set[domain.TemplateId.OptionalPkg],
      q: Map[String, JsValue]): Future[Result] =
    for {
      templateIds <- toFuture(resolveTemplateIds(templateIds))
      allActiveContracts <- getActiveContracts(jwt, transactionFilter(party, templateIds), true)
        .mapAsyncUnordered(parallelism)(gacr =>
          toFuture(domain.GetActiveContractsResponse.fromLedgerApi(gacr)))
        .runWith(Sink.seq)
      predicates = templateIds.iterator.map(a => (a, valuePredicate(a, q))).toMap
    } yield (allActiveContracts, predicates)

  def filterSearch(
      compiledPredicates: CompiledPredicates,
      rawContracts: Seq[domain.GetActiveContractsResponse[V[V.AbsoluteContractId]]])
    : Seq[domain.GetActiveContractsResponse[V[V.AbsoluteContractId]]] = {
    val predFuns = compiledPredicates transform ((_, vp) => vp.toFunPredicate)
    rawContracts map (gacr =>
      gacr copy (activeContracts = gacr.activeContracts.filter(ac =>
        predFuns get ac.templateId forall (_(ac.argument)))))
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
