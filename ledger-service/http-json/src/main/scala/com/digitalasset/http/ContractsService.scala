// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.ledger.api.v1.value.Value
import com.digitalasset.ledger.api.v1.transaction_filter
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import scalaz.\/
import scalaz.std.string._

import scala.concurrent.{ExecutionContext, Future}

class ContractsService(
    templateIdMap: PackageService.TemplateIdMap,
    activeContractSetClient: ActiveContractSetClient)(
    implicit ec: ExecutionContext,
    mat: Materializer) {

  import ContractsService._

  private val resolveTemplateIds: Set[domain.TemplateId.OptionalPkg] => Error \/ List[Identifier] =
    PackageService.resolveTemplateIds(templateIdMap)

  def search(jwtPayload: domain.JwtPayload, request: domain.GetActiveContractsRequest)
    : Future[Seq[domain.GetActiveContractsResponse[Value]]] =
    search(jwtPayload.party, request.templateIds)

  def search(party: String, templateIds: Set[domain.TemplateId.OptionalPkg])
    : Future[Seq[domain.GetActiveContractsResponse[Value]]] =
    for {
      templateIds <- toFuture(resolveTemplateIds(templateIds))
      activeContracts <- activeContractSetClient
        .getActiveContracts(transactionFilter(party, templateIds), verbose = true)
        .mapAsyncUnordered(8)(gacr =>
          toFuture(domain.GetActiveContractsResponse.fromLedgerApi(gacr)))
        .runWith(Sink.seq)
    } yield activeContracts

  private def transactionFilter(party: String, templateIds: List[Identifier]): TransactionFilter = {
    val filters =
      if (templateIds.isEmpty) Filters.defaultInstance
      else Filters(Some(InclusiveFilters(templateIds)))
    transaction_filter.TransactionFilter(Map(party -> filters))
  }
}

object ContractsService {
  type Error = String
}
