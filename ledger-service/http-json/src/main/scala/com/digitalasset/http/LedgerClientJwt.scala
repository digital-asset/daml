// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.ledger.client.LedgerClient

import scala.concurrent.{ExecutionContext, Future}

object LedgerClientJwt {

  type SubmitAndWaitForTransaction =
    (Jwt, SubmitAndWaitRequest) => Future[SubmitAndWaitForTransactionResponse]

  type SubmitAndWaitForTransactionTree =
    (Jwt, SubmitAndWaitRequest) => Future[SubmitAndWaitForTransactionTreeResponse]

  type GetTermination =
    Jwt => Future[Option[Terminates.AtAbsolute]]

  type GetActiveContracts =
    (Jwt, TransactionFilter, Boolean) => Source[GetActiveContractsResponse, NotUsed]

  type GetCreatesAndArchivesSince =
    (Jwt, TransactionFilter, LedgerOffset, Terminates) => Source[Transaction, NotUsed]

  private def bearer(jwt: Jwt): Some[String] = Some(s"Bearer ${jwt.value: String}")

  def submitAndWaitForTransaction(client: LedgerClient): SubmitAndWaitForTransaction =
    (jwt, req) => client.commandServiceClient.submitAndWaitForTransaction(req, bearer(jwt))

  def submitAndWaitForTransactionTree(client: LedgerClient): SubmitAndWaitForTransactionTree =
    (jwt, req) => client.commandServiceClient.submitAndWaitForTransactionTree(req, bearer(jwt))

  def getTermination(client: LedgerClient)(implicit ec: ExecutionContext): GetTermination =
    jwt =>
      client.transactionClient.getLedgerEnd(bearer(jwt)).map {
        _.offset flatMap {
          _.value match {
            case off @ LedgerOffset.Value.Absolute(_) => Some(Terminates.AtAbsolute(off))
            case LedgerOffset.Value.Boundary(_) | LedgerOffset.Value.Empty => None // at beginning
          }
        }
      }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def getActiveContracts(client: LedgerClient): GetActiveContracts =
    (jwt, filter, verbose) =>
      client.activeContractSetClient
        .getActiveContracts(filter, verbose, bearer(jwt))
        .mapMaterializedValue(_ => NotUsed)

  sealed abstract class Terminates extends Product with Serializable {
    import Terminates._
    def toOffset: Option[LedgerOffset] = this match {
      case AtLedgerEnd => Some(ledgerEndOffset)
      case Never => None
      case AtAbsolute(off) => Some(LedgerOffset(off))
    }
  }
  object Terminates {
    case object AtLedgerEnd extends Terminates
    case object Never extends Terminates
    final case class AtAbsolute(off: LedgerOffset.Value.Absolute) extends Terminates
  }

  private val ledgerEndOffset =
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))

  def getCreatesAndArchivesSince(client: LedgerClient): GetCreatesAndArchivesSince =
    (jwt, filter, offset, terminates) =>
      client.transactionClient
        .getTransactions(offset, terminates.toOffset, filter, verbose = true, token = bearer(jwt))
}
