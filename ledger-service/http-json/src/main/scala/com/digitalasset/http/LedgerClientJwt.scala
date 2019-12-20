// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest
}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.ledger.client.LedgerClient

import scala.concurrent.Future

object LedgerClientJwt {

  type SubmitAndWaitForTransaction =
    (Jwt, SubmitAndWaitRequest) => Future[SubmitAndWaitForTransactionResponse]

  type SubmitAndWaitForTransactionTree =
    (Jwt, SubmitAndWaitRequest) => Future[SubmitAndWaitForTransactionTreeResponse]

  type GetActiveContracts =
    (Jwt, TransactionFilter, Boolean) => Source[GetActiveContractsResponse, NotUsed]

  type GetCreatesAndArchivesSince =
    (Jwt, TransactionFilter, LedgerOffset) => Source[Transaction, NotUsed]

  private def bearer(jwt: Jwt): Some[String] = Some(s"Bearer ${jwt.value: String}")

  def submitAndWaitForTransaction(client: LedgerClient): SubmitAndWaitForTransaction =
    (jwt, req) => client.commandServiceClient.submitAndWaitForTransaction(req, bearer(jwt))

  def submitAndWaitForTransactionTree(client: LedgerClient): SubmitAndWaitForTransactionTree =
    (jwt, req) => client.commandServiceClient.submitAndWaitForTransactionTree(req, bearer(jwt))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def getActiveContracts(client: LedgerClient): GetActiveContracts =
    (jwt, filter, flag) =>
      client.activeContractSetClient
        .getActiveContracts(filter, flag, bearer(jwt))
        .mapMaterializedValue(_ => NotUsed)

  def getCreatesAndArchivesSince(client: LedgerClient): GetCreatesAndArchivesSince =
    (jwt, filter, offset) =>
      Source
        .future(client.transactionClient.getLedgerEnd(bearer(jwt)))
        .flatMapConcat(end =>
          client.transactionClient
            .getTransactions(offset, end.offset, filter, verbose = true, token = bearer(jwt)))
}
