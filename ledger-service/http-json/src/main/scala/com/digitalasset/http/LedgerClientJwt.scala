// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api
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
import scalaz.OneAnd

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

  type ListKnownParties =
    Jwt => Future[List[api.domain.PartyDetails]]

  type GetParties =
    (Jwt, OneAnd[Set, Ref.Party]) => Future[List[api.domain.PartyDetails]]

  type AllocateParty =
    (Jwt, Option[Ref.Party], Option[String]) => Future[api.domain.PartyDetails]

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
    (jwt, filter, offset, terminates) => {
      val end = terminates.toOffset
      if (skipRequest(offset, end))
        Source.empty[Transaction]
      else
        client.transactionClient
          .getTransactions(offset, terminates.toOffset, filter, verbose = true, token = bearer(jwt))
    }

  private def skipRequest(start: LedgerOffset, end: Option[LedgerOffset]): Boolean = {
    import com.digitalasset.http.util.LedgerOffsetUtil.AbsoluteOffsetOrdering
    (start.value, end.map(_.value)) match {
      case (s: LedgerOffset.Value.Absolute, Some(e: LedgerOffset.Value.Absolute)) =>
        AbsoluteOffsetOrdering.gteq(s, e)
      case _ => false
    }
  }

  def listKnownParties(client: LedgerClient): ListKnownParties =
    jwt => client.partyManagementClient.listKnownParties(bearer(jwt))

  def getParties(client: LedgerClient): GetParties =
    (jwt, partyIds) => client.partyManagementClient.getParties(partyIds, bearer(jwt))

  def allocateParty(client: LedgerClient): AllocateParty =
    (jwt, identifierHint, displayName) =>
      client.partyManagementClient.allocateParty(
        hint = identifierHint,
        displayName = displayName,
        token = bearer(jwt))
}
