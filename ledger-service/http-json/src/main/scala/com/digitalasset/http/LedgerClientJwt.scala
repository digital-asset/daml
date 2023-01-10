// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.stream.scaladsl.Source
import util.Logging.{InstanceUUID, RequestID}
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
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.google.protobuf
import scalaz.{-\/, OneAnd, \/}
import scalaz.syntax.std.boolean._

import scala.concurrent.{Future, ExecutionContext => EC}
import scala.util.control.NonFatal
import com.daml.ledger.api.{domain => LedgerApiDomain}
import com.daml.ledger.api.v1.admin.metering_report_service.{
  GetMeteringReportRequest,
  GetMeteringReportResponse,
}
import com.daml.ledger.client.services.acs.ActiveContractSetClient
import com.daml.ledger.client.services.admin.{
  MeteringReportClient,
  PackageManagementClient,
  PartyManagementClient,
}
import com.daml.ledger.client.services.commands.SynchronousCommandClient
import com.daml.ledger.client.services.pkg.withoutledgerid.PackageClient
import com.daml.ledger.client.services.transactions.withoutledgerid.TransactionClient
import com.google.rpc.{Code, Status}
import io.grpc.protobuf.StatusProto

object LedgerClientJwt {
  import Grpc.EFuture, Grpc.Category._

  import LedgerClientRequestTimeLogger._

  private[this] val logger = ContextualizedLogger.get(getClass)

  // there are other error categories of interest if we wish to propagate
  // different 5xx errors, but PermissionDenied and InvalidArgument are the only
  // "client" errors here
  type SubmitAndWaitForTransaction =
    (
        Jwt,
        SubmitAndWaitRequest,
    ) => LoggingContextOf[InstanceUUID with RequestID] => EFuture[
      SubmitError,
      SubmitAndWaitForTransactionResponse,
    ]

  type SubmitAndWaitForTransactionTree =
    (
        Jwt,
        SubmitAndWaitRequest,
    ) => LoggingContextOf[InstanceUUID with RequestID] => EFuture[
      SubmitError,
      SubmitAndWaitForTransactionTreeResponse,
    ]

  type GetTermination =
    (
        Jwt,
        LedgerApiDomain.LedgerId,
    ) => LoggingContextOf[InstanceUUID] => Future[Option[Terminates.AtAbsolute]]

  type GetActiveContracts =
    (
        Jwt,
        LedgerApiDomain.LedgerId,
        TransactionFilter,
        Boolean,
    ) => LoggingContextOf[InstanceUUID] => Source[
      GetActiveContractsResponse,
      NotUsed,
    ]

  type GetCreatesAndArchivesSince =
    (
        Jwt,
        LedgerApiDomain.LedgerId,
        TransactionFilter,
        LedgerOffset,
        Terminates,
    ) => LoggingContextOf[InstanceUUID] => Source[Transaction, NotUsed]

  type ListKnownParties =
    Jwt => LoggingContextOf[InstanceUUID with RequestID] => EFuture[PermissionDenied, List[
      api.domain.PartyDetails
    ]]

  type GetParties =
    (
        Jwt,
        OneAnd[Set, Ref.Party],
    ) => LoggingContextOf[InstanceUUID with RequestID] => EFuture[PermissionDenied, List[
      api.domain.PartyDetails
    ]]

  type AllocateParty =
    (
        Jwt,
        Option[Ref.Party],
        Option[String],
    ) => LoggingContextOf[InstanceUUID with RequestID] => Future[api.domain.PartyDetails]

  type ListPackages =
    (Jwt, LedgerApiDomain.LedgerId) => LoggingContextOf[InstanceUUID with RequestID] => Future[
      package_service.ListPackagesResponse
    ]

  type GetPackage =
    (
        Jwt,
        LedgerApiDomain.LedgerId,
        String,
    ) => LoggingContextOf[InstanceUUID with RequestID] => Future[
      package_service.GetPackageResponse
    ]

  type UploadDarFile =
    (
        Jwt,
        LedgerApiDomain.LedgerId,
        protobuf.ByteString,
    ) => LoggingContextOf[InstanceUUID with RequestID] => Future[Unit]

  type GetMeteringReport =
    (Jwt, GetMeteringReportRequest) => LoggingContextOf[InstanceUUID with RequestID] => Future[
      GetMeteringReportResponse
    ]

  private def bearer(jwt: Jwt): Some[String] = Some(jwt.value: String)

  def submitAndWaitForTransaction(
      client: DamlLedgerClient
  )(implicit ec: EC): SubmitAndWaitForTransaction =
    (jwt, req) =>
      implicit lc => {
        LedgerClientRequestTimeLogger
          .logFuture(SubmitAndWaitForTransactionLog) {
            client.commandServiceClient
              .submitAndWaitForTransaction(req, bearer(jwt))
          }
          .requireHandling(submitErrors)
      }
  def submitAndWaitForTransactionTree(
      client: DamlLedgerClient
  )(implicit ec: EC): SubmitAndWaitForTransactionTree =
    (jwt, req) =>
      implicit lc => {
        LedgerClientRequestTimeLogger
          .logFuture(SubmitAndWaitForTransactionTreeLog) {
            client.commandServiceClient
              .submitAndWaitForTransactionTree(req, bearer(jwt))
          }
          .requireHandling(submitErrors)
      }

  def getTermination(client: DamlLedgerClient)(implicit
      ec: EC
  ): GetTermination =
    (jwt, ledgerId) =>
      implicit lc => {
        LedgerClientRequestTimeLogger
          .logFuture(GetLedgerEndLog) {
            client.transactionClient.getLedgerEnd(ledgerId, bearer(jwt))
          }
          .map {
            _.offset flatMap {
              _.value match {
                case off @ LedgerOffset.Value.Absolute(_) => Some(Terminates.AtAbsolute(off))
                case LedgerOffset.Value.Boundary(_) | LedgerOffset.Value.Empty =>
                  None // at beginning
              }
            }
          }
      }

  def getActiveContracts(client: DamlLedgerClient): GetActiveContracts =
    (jwt, ledgerId, filter, verbose) =>
      implicit lc => {
        LedgerClientRequestTimeLogger
          .log(GetActiveContractsLog) {
            client.activeContractSetClient
              .getActiveContracts(filter, ledgerId, verbose, bearer(jwt))
              .mapMaterializedValue(_ => NotUsed)
          }
      }

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
    def fromDomain(o: domain.Offset): AtAbsolute =
      AtAbsolute(
        LedgerOffset.Value.Absolute(domain.Offset unwrap o)
      )
  }

  private val ledgerEndOffset =
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))

  def getCreatesAndArchivesSince(client: DamlLedgerClient): GetCreatesAndArchivesSince =
    (jwt, ledgerId, filter, offset, terminates) => { implicit lc =>
      {
        val end = terminates.toOffset
        if (skipRequest(offset, end))
          Source.empty[Transaction]
        else {
          LedgerClientRequestTimeLogger.log(GetTransactionsLog) {
            client.transactionClient
              .getTransactions(
                offset,
                terminates.toOffset,
                filter,
                ledgerId,
                verbose = true,
                token = bearer(jwt),
              )
          }
        }
      }
    }

  private def skipRequest(start: LedgerOffset, end: Option[LedgerOffset]): Boolean = {
    import com.daml.http.util.LedgerOffsetUtil.AbsoluteOffsetOrdering
    (start.value, end.map(_.value)) match {
      case (s: LedgerOffset.Value.Absolute, Some(e: LedgerOffset.Value.Absolute)) =>
        AbsoluteOffsetOrdering.gteq(s, e)
      case _ => false
    }
  }

  def listKnownParties(client: DamlLedgerClient)(implicit
      ec: EC
  ): ListKnownParties =
    jwt =>
      implicit lc => {
        LedgerClientRequestTimeLogger
          .logFuture(ListKnownPartiesLog) {
            client.partyManagementClient.listKnownParties(bearer(jwt))
          }
          .requireHandling { case Code.PERMISSION_DENIED =>
            PermissionDenied
          }
      }

  def getParties(client: DamlLedgerClient)(implicit
      ec: EC
  ): GetParties =
    (jwt, partyIds) =>
      implicit lc => {
        LedgerClientRequestTimeLogger
          .logFuture(GetPartiesLog) {
            client.partyManagementClient.getParties(partyIds, bearer(jwt))
          }
          .requireHandling { case Code.PERMISSION_DENIED =>
            PermissionDenied
          }
      }

  def allocateParty(client: DamlLedgerClient)(implicit
      ec: EC
  ): AllocateParty =
    (jwt, identifierHint, displayName) =>
      implicit lc => {
        LedgerClientRequestTimeLogger.logFuture(AllocatePartyLog) {
          client.partyManagementClient.allocateParty(
            hint = identifierHint,
            displayName = displayName,
            token = bearer(jwt),
          )
        }
      }

  def listPackages(client: DamlLedgerClient)(implicit
      ec: EC
  ): ListPackages =
    (jwt, ledgerId) =>
      implicit lc => {
        logger.trace("sending list packages request to ledger")
        LedgerClientRequestTimeLogger.logFuture(ListPackagesLog) {
          client.packageClient.listPackages(ledgerId, bearer(jwt))
        }
      }

  def getPackage(client: DamlLedgerClient)(implicit
      ec: EC
  ): GetPackage =
    (jwt, ledgerId, packageId) =>
      implicit lc => {
        logger.trace("sending get packages request to ledger")
        LedgerClientRequestTimeLogger.logFuture(GetPackageLog) {
          client.packageClient.getPackage(packageId, ledgerId, token = bearer(jwt))
        }
      }

  def uploadDar(client: DamlLedgerClient)(implicit
      ec: EC
  ): UploadDarFile =
    (jwt, _, byteString) =>
      implicit lc => {
        logger.trace("sending upload dar request to ledger")
        LedgerClientRequestTimeLogger.logFuture(UploadDarFileLog) {
          client.packageManagementClient.uploadDarFile(darFile = byteString, token = bearer(jwt))
        }
      }

  def getMeteringReport(client: DamlLedgerClient)(implicit
      ec: EC
  ): GetMeteringReport =
    (jwt, request) =>
      implicit lc => {
        logger.trace("sending metering report request to ledger")
        LedgerClientRequestTimeLogger.logFuture(GetMeteringReportLog) {
          client.meteringReportClient.getMeteringReport(request, bearer(jwt))
        }
      }

  // a shim error model to stand in for https://github.com/digital-asset/daml/issues/9834
  object Grpc {
    type EFuture[E, A] = Future[Error[E] \/ A]

    final case class Error[+E](e: E, message: String)

    private[http] object StatusEnvelope {
      def unapply(t: Throwable): Option[Status] = t match {
        case NonFatal(t) =>
          val status = StatusProto fromThrowable t
          if (status == null) None
          else {
            // fromThrowable uses UNKNOWN if it didn't find one
            val code = com.google.rpc.Code.forNumber(status.getCode)
            if (code == null) None
            else (code != com.google.rpc.Code.UNKNOWN) option status
          }
        case _ => None
      }
    }

    // like Code but with types
    // only needs to contain types that may be reported to the json-api user;
    // if it is an "internal error" there is no need to call it out for handling
    // e.g. Unauthenticated never needs to be specially handled, because we should
    // have caught that the jwt token was missing and reported that to client already
    object Category {
      sealed trait SubmitError
      // XXX SC we might be able to assign singleton types to the Codes instead in 2.13+
      type PermissionDenied = PermissionDenied.type
      case object PermissionDenied extends SubmitError
      type InvalidArgument = InvalidArgument.type
      case object InvalidArgument extends SubmitError
      // not *every* singleton here should be a subtype of SubmitError;
      // think of it more like a Venn diagram

      private[LedgerClientJwt] val submitErrors: Code PartialFunction SubmitError = {
        case Code.PERMISSION_DENIED => PermissionDenied
        case Code.INVALID_ARGUMENT => InvalidArgument
      }

      private[LedgerClientJwt] implicit final class `Future Status Category ops`[A](
          private val fa: Future[A]
      ) extends AnyVal {
        def requireHandling[E](c: Code PartialFunction E)(implicit ec: EC): EFuture[E, A] =
          fa map \/.right[Error[E], A] recover Function.unlift {
            case StatusEnvelope(status) =>
              c.lift(Code.forNumber(status.getCode)) map (e => -\/(Error(e, status.getMessage)))
            case _ => None
          }
      }
    }
  }

  object LedgerClientRequestTimeLogger {
    sealed abstract class RequestLog(klass: Class[_], val requestName: String)
        extends Product
        with Serializable {
      final def className: String = klass.getSimpleName
    }

    case object SubmitAndWaitForTransactionLog
        extends RequestLog(classOf[SynchronousCommandClient], "submitAndWaitForTransaction")
    case object SubmitAndWaitForTransactionTreeLog
        extends RequestLog(classOf[SynchronousCommandClient], "submitAndWaitForTransactionTree")
    case object GetLedgerEndLog extends RequestLog(classOf[TransactionClient], "getLedgerEnd")
    case object ListKnownPartiesLog
        extends RequestLog(classOf[PartyManagementClient], "listKnownParties")
    case object GetPartiesLog extends RequestLog(classOf[PartyManagementClient], "getParties")
    case object AllocatePartyLog extends RequestLog(classOf[PartyManagementClient], "allocateParty")
    case object ListPackagesLog extends RequestLog(classOf[PackageClient], "listPackages")
    case object GetPackageLog extends RequestLog(classOf[PackageClient], "getPackages")
    case object UploadDarFileLog
        extends RequestLog(classOf[PackageManagementClient], "uploadDarFile")
    case object GetMeteringReportLog
        extends RequestLog(classOf[MeteringReportClient], "getMeteringReport")
    case object GetActiveContractsLog
        extends RequestLog(classOf[ActiveContractSetClient], "getActiveContracts")
    case object GetTransactionsLog extends RequestLog(classOf[TransactionClient], "getTransactions")

    private[LedgerClientJwt] def logMessage(startTime: Long, requestLog: RequestLog): String = {
      s"Ledger client request ${requestLog.className} ${requestLog.requestName} executed, elapsed time: " +
        s"${(System.nanoTime() - startTime) / 1000000L} ms"
    }

    private[http] def logFuture[T, C](
        requestLog: RequestLog
    )(block: => Future[T])(implicit ec: EC, lc: LoggingContextOf[C]): Future[T] = if (
      logger.debug.isEnabled
    ) {
      val start = System.nanoTime()
      val futureResult = block
      futureResult.andThen { case _ =>
        logger.debug(logMessage(start, requestLog))
      }
    } else block

    private[http] def log[T, C](
        requestLog: RequestLog
    )(block: => T)(implicit lc: LoggingContextOf[C]): T = if (logger.debug.isEnabled) {
      val start = System.nanoTime()
      val result = block
      logger.debug(logMessage(start, requestLog))
      result
    } else block
  }
}
