// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.digitalasset.http.ContractsService._
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.v1.value.{Identifier, Value}
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import scalaz.\/
import scalaz.std.string._

import scala.concurrent.{ExecutionContext, Future}

class ContractsService(
    resolveTemplateIds: ResolveTemplateIds,
    activeContractSetClient: ActiveContractSetClient,
    parallelism: Int = 8)(implicit ec: ExecutionContext, mat: Materializer) {

  def lookup(
      jwtPayload: domain.JwtPayload,
      request: domain.ContractLookupRequest[Value]): Future[Option[domain.ActiveContract[Value]]] =
    Future.failed(new RuntimeException("contract lookup not yet supported")) // TODO

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
  type Error = String
  type ResolveTemplateIds = Set[domain.TemplateId.OptionalPkg] => Error \/ List[Identifier]
}
