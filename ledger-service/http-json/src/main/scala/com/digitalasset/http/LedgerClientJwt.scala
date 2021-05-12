// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api
import com.daml.ledger.api.v1.package_service
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.client.LedgerClient
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.google.protobuf
import scalaz.OneAnd

import scala.concurrent.{ExecutionContext, Future}

object LedgerClientJwt {

  private[this] val logger = ContextualizedLogger.get(getClass)

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

  type ListPackages =
    Jwt => LoggingContextOf[InstanceUUID with RequestID] => Future[
      package_service.ListPackagesResponse
    ]

  type GetPackage =
    (Jwt, String) => LoggingContextOf[InstanceUUID with RequestID] => Future[
      package_service.GetPackageResponse
    ]

  type UploadDarFile =
    (Jwt, protobuf.ByteString) => LoggingContextOf[InstanceUUID with RequestID] => Future[Unit]

  private def bearer(jwt: Jwt): Some[String] = Some(jwt.value: String)

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
    final case class AtAbsolute(off: LedgerOffset.Value.Absolute) extends Terminates {
      def toDomain: domain.Offset = domain.Offset(off.value)
    }
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
    import com.daml.http.util.LedgerOffsetUtil.AbsoluteOffsetOrdering
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
        token = bearer(jwt),
      )

  def listPackages(client: LedgerClient): ListPackages =
    jwt =>
      implicit lc => {
        logger.trace("sending list packages request to ledger")
        client.packageClient.listPackages(bearer(jwt))
      }

  def getPackage(client: LedgerClient): GetPackage =
    (jwt, packageId) =>
      implicit lc => {
        logger.trace("sending get packages request to ledger")
        client.packageClient.getPackage(packageId, token = bearer(jwt))
      }

  def uploadDar(client: LedgerClient): UploadDarFile =
    (jwt, byteString) =>
      implicit lc => {
        logger.trace("sending upload dar request to ledger")
        client.packageManagementClient.uploadDarFile(darFile = byteString, token = bearer(jwt))
      }
}
