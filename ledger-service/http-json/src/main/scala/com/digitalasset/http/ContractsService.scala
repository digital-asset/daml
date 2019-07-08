// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.digitalasset.http.ContractsService._
import com.digitalasset.http.domain.TemplateId
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.v1.value.{Identifier, Value}
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import scalaz.{-\/, \/, \/-}
import scalaz.std.string._

import scala.concurrent.{ExecutionContext, Future}

class ContractsService(
    resolveTemplateIds: ResolveTemplateIds,
    activeContractSetClient: ActiveContractSetClient,
    parallelism: Int = 8)(implicit ec: ExecutionContext, mat: Materializer) {

  def lookup(
      jwtPayload: domain.JwtPayload,
      request: domain.ContractLookupRequest[Value]): Future[Option[domain.ActiveContract[Value]]] =
    request.id match {
      case -\/((templateId, contractKey)) =>
        lookup(jwtPayload.party, templateId, contractKey)
      case \/-((templateId, contractId)) =>
        lookup(jwtPayload.party, templateId, contractId)
    }

  def lookup(
      party: String,
      templateId: TemplateId.OptionalPkg,
      contractKey: Value): Future[Option[domain.ActiveContract[Value]]] =
    for {
      as <- search(party, Set(templateId))
      a = findByContractKey(contractKey)(as)
    } yield a

  private def findByContractKey(k: Value)(
      as: Seq[domain.GetActiveContractsResponse[Value]]): Option[domain.ActiveContract[Value]] =
    (as.view: Seq[domain.GetActiveContractsResponse[Value]])
      .flatMap(a => a.activeContracts)
      .find(isContractKey(k))

  private def isContractKey(k: Value)(a: domain.ActiveContract[Value]): Boolean =
    a.key.fold(false)(_ == k)

  def lookup(
      party: String,
      templateId: Option[TemplateId.OptionalPkg],
      contractId: String): Future[Option[domain.ActiveContract[Value]]] =
    for {
      as <- search(party, templateIds(templateId))
      a = findByContractId(contractId)(as)
    } yield a

  private def templateIds(a: Option[TemplateId.OptionalPkg]): Set[TemplateId.OptionalPkg] =
    a.fold(Set.empty[TemplateId.OptionalPkg])(x => Set(x))

  private def findByContractId(k: String)(
      as: Seq[domain.GetActiveContractsResponse[Value]]): Option[domain.ActiveContract[Value]] =
    (as.view: Seq[domain.GetActiveContractsResponse[Value]])
      .flatMap(a => a.activeContracts)
      .find(x => x.contractId == k)

  def search(jwtPayload: domain.JwtPayload, request: domain.GetActiveContractsRequest)
    : Future[Seq[domain.GetActiveContractsResponse[Value]]] =
    search(jwtPayload.party, request.templateIds)

  def search(party: String, templateIds: Set[domain.TemplateId.OptionalPkg])
    : Future[Seq[domain.GetActiveContractsResponse[Value]]] =
    for {
      templateIds <- toFuture(resolveTemplateIds(templateIds))
      activeContracts <- activeContractSetClient
        .getActiveContracts(transactionFilter(party, templateIds), verbose = true)
        .mapAsyncUnordered(parallelism)(gacr =>
          toFuture(domain.GetActiveContractsResponse.fromLedgerApi(gacr)))
        .runWith(Sink.seq)
    } yield activeContracts

  private def transactionFilter(party: String, templateIds: List[Identifier]): TransactionFilter = {
    val filters =
      if (templateIds.isEmpty) Filters.defaultInstance
      else Filters(Some(InclusiveFilters(templateIds)))
    TransactionFilter(Map(party -> filters))
  }
}

object ContractsService {
  final case class Error(message: String)

  type ResolveTemplateIds =
    Set[domain.TemplateId.OptionalPkg] => PackageService.Error \/ List[Identifier]
}
