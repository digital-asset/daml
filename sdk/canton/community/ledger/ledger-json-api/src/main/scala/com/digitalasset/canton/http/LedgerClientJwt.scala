// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.jwt.Jwt
import com.daml.ledger.api.v2.admin.metering_report_service.{
  GetMeteringReportRequest,
  GetMeteringReportResponse,
}
import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.ledger.api.v2.package_service
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse.Update
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.http.LedgerClientJwt.Grpc
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.digitalasset.canton.ledger.api.domain.PartyDetails as domainPartyDetails
import com.digitalasset.canton.ledger.client.LedgerClient as DamlLedgerClient
import com.digitalasset.canton.ledger.client.services.EventQueryServiceClient
import com.digitalasset.canton.ledger.client.services.admin.{
  MeteringReportClient,
  PackageManagementClient,
  PartyManagementClient,
}
import com.digitalasset.canton.ledger.client.services.commands.CommandServiceClient
import com.digitalasset.canton.ledger.client.services.pkg.PackageClient
import com.digitalasset.canton.ledger.client.services.state.StateServiceClient
import com.digitalasset.canton.ledger.client.services.updates.UpdateServiceClient
import com.digitalasset.canton.ledger.service.Grpc.StatusEnvelope
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.ApiOffset
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf
import com.google.rpc.Code
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import scalaz.syntax.tag.*
import scalaz.{-\/, OneAnd, \/}

import scala.concurrent.{ExecutionContext as EC, Future}

final case class LedgerClientJwt(loggerFactory: NamedLoggerFactory) extends NamedLogging {
  import Grpc.Category.*
  import LedgerClientJwt.*
  import LedgerClientRequestTimeLogger.*

  private def bearer(jwt: Jwt): Some[String] = Some(jwt.value: String)

  def submitAndWaitForTransaction(
      client: DamlLedgerClient
  )(implicit ec: EC): SubmitAndWaitForTransaction =
    (jwt, req) =>
      implicit traceContext =>
        implicit lc => {
          logFuture(SubmitAndWaitForTransactionLog) {
            client.commandService
              .deprecatedSubmitAndWaitForTransactionForJsonApi(req, token = bearer(jwt))
          }
            .requireHandling(submitErrors)
        }

  def submitAndWaitForTransactionTree(
      client: DamlLedgerClient
  )(implicit ec: EC, traceContext: TraceContext): SubmitAndWaitForTransactionTree =
    (jwt, req) =>
      implicit lc => {
        logFuture(SubmitAndWaitForTransactionTreeLog) {
          client.commandService
            .deprecatedSubmitAndWaitForTransactionTreeForJsonApi(req, token = bearer(jwt))
        }
          .requireHandling(submitErrors)
      }

  // TODO(#13364) test this function with a token or do not pass the token to getActiveContractsSource if it is not needed
  def getActiveContracts(client: DamlLedgerClient)(implicit
      traceContext: TraceContext
  ): GetActiveContracts =
    (jwt, filter, offset, verbose) =>
      implicit lc => {
        log(GetActiveContractsLog) {
          client.stateService
            .getActiveContractsSource(
              filter = filter,
              token = bearer(jwt),
              verbose = verbose,
              validAtOffset = offset,
            )
        }
      }

  def getCreatesAndArchivesSince(
      client: DamlLedgerClient
  )(implicit traceContext: TraceContext): GetCreatesAndArchivesSince =
    (jwt, filter, offset, terminates) => { implicit lc =>
      val endSource: Source[Option[Long], NotUsed] = terminates match {
        case Terminates.AtParticipantEnd =>
          Source
            .future(client.stateService.getLedgerEnd())
            .map(_.offset)
            .map(Some(_))
        case Terminates.Never => Source.single(None)
        case Terminates.AtAbsolute(off) =>
          Source.single(Some(ApiOffset.assertFromStringToLong(off)))
      }
      endSource.flatMapConcat { end =>
        if (skipRequest(offset, end))
          Source.empty[Transaction]
        else {
          log(GetUpdatesLog) {
            client.updateService
              .getUpdatesSource(
                begin = offset,
                filter = filter,
                verbose = true,
                end = end,
                token = bearer(jwt),
              )
              .collect { response =>
                response.update match {
                  case Update.Transaction(t) => t
                }
              }
          }
        }
      }
    }

  def getByContractId(
      client: DamlLedgerClient
  )(implicit ec: EC, traceContext: TraceContext): GetContractByContractId = {
    (jwt, contractId, requestingParties) => implicit lc =>
      logFuture(GetContractByContractIdLog) {
        client.eventQueryService.getEventsByContractId(
          contractId = contractId.unwrap,
          requestingParties = requestingParties.view.map(_.unwrap).toSeq,
          token = bearer(jwt),
        )
      }
        .requireHandling { case Code.PERMISSION_DENIED =>
          PermissionDenied
        }
  }

  //  TODO(#16065)
  //  def getByContractKey(client: DamlLedgerClient)(implicit ec: EC): GetContractByContractKey = {
  //    (jwt, key, templateId, requestingParties, continuationToken) =>
  //      { implicit lc =>
  //        logFuture(GetContractByContractKeyLog) {
  //          client.eventQueryServiceClient.getEventsByContractKey(
  //            token = bearer(jwt),
  //            contractKey = key,
  //            templateId = templateId,
  //            requestingParties = requestingParties.view.map(_.unwrap).toSeq,
  //            continuationToken = continuationToken,
  //          )
  //        }
  //          .requireHandling { case Code.PERMISSION_DENIED =>
  //            PermissionDenied
  //          }
  //      }
  //  }

  private def skipRequest(start: Long, endO: Option[Long]): Boolean =
    (start, endO) match {
      case (_, Some(end)) => start >= end
      case (_, None) => false
    }

  // TODO(#13303): Replace all occurrences of EC for logging purposes in this file
  //  (preferrably with DirectExecutionContext)
  def listKnownParties(client: DamlLedgerClient)(implicit
      ec: EC,
      traceContext: TraceContext,
  ): ListKnownParties =
    (jwt, pageToken, pageSize) =>
      implicit lc => {
        logFuture(ListKnownPartiesLog) {
          client.partyManagementClient.listKnownParties(bearer(jwt), pageToken, pageSize)
        }
          .requireHandling { case Code.PERMISSION_DENIED =>
            PermissionDenied
          }
      }

  def getParties(client: DamlLedgerClient)(implicit
      ec: EC,
      traceContext: TraceContext,
  ): GetParties =
    (jwt, partyIds) =>
      implicit lc => {
        logFuture(GetPartiesLog) {
          client.partyManagementClient.getParties(partyIds, bearer(jwt))
        }
          .requireHandling { case Code.PERMISSION_DENIED =>
            PermissionDenied
          }
      }

  def allocateParty(client: DamlLedgerClient)(implicit
      ec: EC,
      traceContext: TraceContext,
  ): AllocateParty =
    (jwt, identifierHint) =>
      implicit lc => {
        logFuture(AllocatePartyLog) {
          client.partyManagementClient.allocateParty(
            hint = identifierHint,
            token = bearer(jwt),
          )
        }
      }

  def listPackages(client: DamlLedgerClient)(implicit
      ec: EC,
      traceContext: TraceContext,
  ): ListPackages =
    jwt =>
      implicit lc => {
        logger.trace(s"sending list packages request to ledger, ${lc.makeString}")
        logFuture(ListPackagesLog) {
          client.packageService.listPackages(bearer(jwt))
        }
      }

  def getPackage(client: DamlLedgerClient)(implicit
      ec: EC,
      traceContext: TraceContext,
  ): GetPackage =
    (jwt, packageId) =>
      implicit lc => {
        logger.trace(s"sending get packages request to ledger, ${lc.makeString}")
        logFuture(GetPackageLog) {
          client.packageService.getPackage(packageId, token = bearer(jwt))
        }
      }

  def uploadDar(client: DamlLedgerClient)(implicit
      ec: EC,
      traceContext: TraceContext,
  ): UploadDarFile =
    (jwt, byteString) =>
      implicit lc => {
        logger.trace(s"sending upload dar request to ledger, ${lc.makeString}")
        logFuture(UploadDarFileLog) {
          client.packageManagementClient.uploadDarFile(darFile = byteString, token = bearer(jwt))
        }
      }

  def getMeteringReport(client: DamlLedgerClient)(implicit
      ec: EC,
      traceContext: TraceContext,
  ): GetMeteringReport =
    (jwt, request) =>
      implicit lc => {
        logger.trace(s"sending metering report request to ledger, ${lc.makeString}")
        logFuture(GetMeteringReportLog) {
          client.meteringReportClient.getMeteringReport(request, bearer(jwt))
        }
      }

  def getLedgerEnd(client: DamlLedgerClient)(implicit
      traceContext: TraceContext
  ): GetLedgerEnd =
    jwt =>
      implicit lc => {
        Source.future(
          log(GetLedgerEndLog) {
            client.stateService.getLedgerEndOffset(token = bearer(jwt))
          }
        )
      }

  private def logFuture[T, C](
      requestLog: RequestLog
  )(
      block: => Future[T]
  )(implicit ec: EC, lc: LoggingContextOf[C], traceContext: TraceContext): Future[T] = if (
    logger.underlying.isDebugEnabled
  ) {
    val start = System.nanoTime()
    val futureResult = block
    futureResult.andThen { case _ =>
      logger.debug(s"${logMessage(start, requestLog)}, ${lc.makeString}")
    }
  } else block

  private def log[T, C](
      requestLog: RequestLog
  )(block: => T)(implicit lc: LoggingContextOf[C], traceContext: TraceContext): T = if (
    logger.underlying.isDebugEnabled
  ) {
    val start = System.nanoTime()
    val result = block
    logger.debug(s"${logMessage(start, requestLog)}, ${lc.makeString}")
    result
  } else block

}
object LedgerClientJwt {
  import Grpc.Category.*
  import Grpc.EFuture

  // there are other error categories of interest if we wish to propagate
  // different 5xx errors, but PermissionDenied and InvalidArgument are the only
  // "client" errors here
  type SubmitAndWaitForTransaction =
    (
        Jwt,
        SubmitAndWaitRequest,
    ) => TraceContext => LoggingContextOf[InstanceUUID with RequestID] => EFuture[
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

  type GetActiveContracts =
    (
        Jwt,
        TransactionFilter,
        Long,
        Boolean,
    ) => LoggingContextOf[InstanceUUID] => Source[
      GetActiveContractsResponse,
      NotUsed,
    ]

  type GetLedgerEnd =
    Jwt => LoggingContextOf[InstanceUUID] => Source[Long, NotUsed]

  type GetCreatesAndArchivesSince =
    (
        Jwt,
        TransactionFilter,
        Long,
        Terminates,
    ) => LoggingContextOf[InstanceUUID] => Source[Transaction, NotUsed]

  type GetContractByContractId =
    (
        Jwt,
        domain.ContractId,
        Set[domain.Party],
    ) => LoggingContextOf[InstanceUUID] => EFuture[PermissionDenied, GetEventsByContractIdResponse]

  //  TODO(#16065)
  //  type ContinuationToken = String
  //  type GetContractByContractKey =
  //    (
  //        Jwt,
  //        com.daml.ledger.api.v2.value.Value,
  //        Identifier,
  //        Set[domain.Party],
  //        ContinuationToken,
  //    ) => LoggingContextOf[InstanceUUID] => EFuture[PermissionDenied, GetEventsByContractKeyResponse]

  type ListKnownParties =
    (
        Jwt,
        String,
        Int,
    ) => LoggingContextOf[InstanceUUID with RequestID] => EFuture[
      PermissionDenied,
      (
          List[
            domainPartyDetails
          ],
          String,
      ),
    ]

  type GetParties =
    (
        Jwt,
        OneAnd[Set, Ref.Party],
    ) => LoggingContextOf[InstanceUUID with RequestID] => EFuture[PermissionDenied, List[
      domainPartyDetails
    ]]

  type AllocateParty =
    (
        Jwt,
        Option[Ref.Party],
    ) => LoggingContextOf[InstanceUUID with RequestID] => Future[domainPartyDetails]

  type ListPackages =
    Jwt => LoggingContextOf[InstanceUUID with RequestID] => Future[
      package_service.ListPackagesResponse
    ]

  type GetPackage =
    (
        Jwt,
        String,
    ) => LoggingContextOf[InstanceUUID with RequestID] => Future[
      package_service.GetPackageResponse
    ]

  type UploadDarFile =
    (
        Jwt,
        protobuf.ByteString,
    ) => LoggingContextOf[InstanceUUID with RequestID] => Future[Unit]

  type GetMeteringReport =
    (Jwt, GetMeteringReportRequest) => LoggingContextOf[InstanceUUID with RequestID] => Future[
      GetMeteringReportResponse
    ]

  sealed abstract class Terminates extends Product with Serializable

  object Terminates {
    // TODO(#21801) remove AtParticipantEnd
    case object AtParticipantEnd extends Terminates
    case object Never extends Terminates
    final case class AtAbsolute(off: String) extends Terminates
  }

  // a shim error model to stand in for https://github.com/digital-asset/daml/issues/9834
  object Grpc {
    type EFuture[E, A] = Future[Error[E] \/ A]

    final case class Error[+E](e: E, message: String)

    // like Code but with types
    // only needs to contain types that may be reported to the json-api user;
    // if it is an "internal error" there is no need to call it out for handling
    // e.g. Unauthenticated never needs to be specially handled, because we should
    // have caught that the jwt token was missing and reported that to client already
    object Category {
      sealed trait SubmitError
      // TODO(i13378) we might be able to assign singleton types to the Codes instead in 2.13+
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
        extends RequestLog(classOf[CommandServiceClient], "submitAndWaitForTransaction")
    case object SubmitAndWaitForTransactionTreeLog
        extends RequestLog(classOf[CommandServiceClient], "submitAndWaitForTransactionTree")
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
        extends RequestLog(classOf[StateServiceClient], "getActiveContracts")
    case object GetLedgerEndLog extends RequestLog(classOf[StateServiceClient], "getLedgerEnd")
    case object GetUpdatesLog extends RequestLog(classOf[UpdateServiceClient], "getUpdates")
    case object GetContractByContractIdLog
        extends RequestLog(classOf[EventQueryServiceClient], "getContractByContractId")
//    TODO(#16065)
//    case object GetContractByContractKeyLog
//        extends RequestLog(classOf[EventQueryServiceClient], "getContractByContractKey")

    private[LedgerClientJwt] def logMessage(startTime: Long, requestLog: RequestLog): String =
      s"Ledger client request ${requestLog.className} ${requestLog.requestName} executed, elapsed time: " +
        s"${(System.nanoTime() - startTime) / 1000000L} ms"
  }
}
