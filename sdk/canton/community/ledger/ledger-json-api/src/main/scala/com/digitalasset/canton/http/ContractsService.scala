// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.jwt.Jwt
import com.daml.ledger.api.v2 as lav2
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.daml.scalautil.ExceptionOps.*
import com.digitalasset.canton.fetchcontracts.AcsTxStreams.transactionFilter
import com.digitalasset.canton.fetchcontracts.util.ContractStreamStep.{Acs, LiveBegin}
import com.digitalasset.canton.fetchcontracts.util.GraphExtensions.*
import com.digitalasset.canton.fetchcontracts.util.{
  AbsoluteBookmark,
  ContractStreamStep,
  InsertDeleteStep,
}
import com.digitalasset.canton.http.Endpoints.ET
import com.digitalasset.canton.http.EndpointsCompanion.NotFound
import com.digitalasset.canton.http.LedgerClientJwt.Terminates
import com.digitalasset.canton.http.PackageService.ResolveContractTypeId.Overload
import com.digitalasset.canton.http.json.JsonProtocol.LfValueCodec
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.http.util.FutureUtil.{either, eitherT}
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.digitalasset.canton.http.{
  ActiveContract,
  ContractTypeId,
  GetActiveContractsRequest,
  JwtPayload,
  Offset,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.daml.lf
import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.stream.scaladsl.*
import scalaz.std.scalaFuture.*
import scalaz.syntax.show.*
import scalaz.syntax.std.option.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, EitherT, Show, \/, \/-}
import spray.json.JsValue

import scala.concurrent.{ExecutionContext, Future}

import util.ApiValueToLfValueConverter

class ContractsService(
    resolveContractTypeId: PackageService.ResolveContractTypeId,
    allTemplateIds: PackageService.AllTemplateIds,
    getContractByContractId: LedgerClientJwt.GetContractByContractId,
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    getLedgerEnd: LedgerClientJwt.GetLedgerEnd,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with NoTracing {
  import ContractsService.*

  private type ActiveContractO = Option[ActiveContract.ResolvedCtTyId[JsValue]]

  def resolveContractReference(
      jwt: Jwt,
      parties: PartySet,
      contractLocator: ContractLocator[LfValue],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[ResolvedContractRef[LfValue]] = {
    import Overload.Template
    contractLocator match {
      case EnrichedContractKey(templateId, key) =>
        _resolveContractTypeId(jwt, templateId).map(x => -\/(x.original -> key))
      case EnrichedContractId(Some(templateId), contractId) =>
        _resolveContractTypeId(jwt, templateId).map(x => \/-(x.original -> contractId))
      case EnrichedContractId(None, contractId) =>
        findByContractId(jwt, parties, contractId)
          .flatMap {
            case Some(value) =>
              EitherT.pure(value): ET[ActiveContract.ResolvedCtTyId[JsValue]]
            case None =>
              EitherT.pureLeft(
                invalidUserInput(
                  s"Could not resolve contract reference for contract id $contractId"
                )
              ): ET[
                ActiveContract.ResolvedCtTyId[JsValue]
              ]
          }
          .map(a => \/-(a.templateId.map(lf.data.Ref.PackageRef.Id.apply) -> a.contractId))
    }
  }

  def lookup(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      req: FetchRequest[LfValue],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[ActiveContractO] = {
    val readAs = req.readAs.cata(_.toSet1, jwtPayload.parties)
    req.locator match {
      case EnrichedContractKey(_templateId, _contractKey) =>
        either(-\/(NotFound("lookup by contract key is not supported")))
      //  TODO(#16065)
      //  findByContractKey(jwt, readAs, templateId, contractKey)
      case EnrichedContractId(_templateId, contractId) =>
        findByContractId(jwt, readAs, contractId)
    }
  }

//  TODO(#16065)
//  private[this] def findByContractKey(
//      jwt: Jwt,
//      parties: PartySet,
//      templateId: ContractTypeId.Template.RequiredPkg,
//      contractKey: LfValue,
//  )(implicit
//      lc: LoggingContextOf[InstanceUUID with RequestID],
//      metrics: HttpApiMetrics,
//  ): ET[ActiveContractO] =
//    timedETFuture(metrics.dbFindByContractKey)(
//      for {
//        resolvedTemplateId <- _resolveContractTypeId(
//          jwt,
//          templateId,
//        )
//        keyApiValue <-
//          EitherT(
//            Future.successful(
//              \/.fromEither(
//                LfEngineToApi.lfValueToApiValue(verbose = true, contractKey)
//              ).leftMap { err =>
//                serverError(s"Cannot convert key $contractKey from LF value to API value: $err")
//              }
//            )
//          )
//        response <- EitherT(
//          getContractByContractKey(
//            jwt,
//            keyApiValue,
//            IdentifierConverters.apiIdentifier(resolvedTemplateId),
//            parties,
//            "",
//          )(lc).map(_.leftMap { err =>
//            unauthorized(
//              s"Unauthorized access for fetching contract with key $contractKey for parties $parties: $err"
//            )
//          })
//        )
//        result <- response match {
//          case GetEventsByContractKeyResponse(None, Some(_), _) =>
//            EitherT.pureLeft(
//              serverError(
//                s"Found archived event in response without a matching create for key $contractKey"
//              )
//            ): ET[ActiveContractO]
//          case GetEventsByContractKeyResponse(_, Some(_), _) |
//              GetEventsByContractKeyResponse(None, None, _) =>
//            logger.debug(s"Contract archived for contract key $contractKey")
//            EitherT.pure(None): ET[ActiveContractO]
//          case GetEventsByContractKeyResponse(Some(createdEvent), None, _) =>
//            EitherT.either(
//              ActiveContract
//                .fromLedgerApi(ActiveContract.IgnoreInterface, createdEvent)
//                .leftMap(_.shows)
//                .flatMap(apiAcToLfAc(_).leftMap(_.shows))
//                .flatMap(_.traverse(lfValueToJsValue(_).leftMap(_.shows)))
//                .leftMap { err =>
//                  serverError(s"Error processing create event for active contract: $err")
//                }
//                .map(Some(_))
//            ): ET[ActiveContractO]
//        }
//      } yield result
//    )

  private[this] def findByContractId(
      jwt: Jwt,
      parties: PartySet,
      contractId: ContractId,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[ActiveContractO] =
    timedETFuture(metrics.dbFindByContractId)(
      eitherT(
        getContractByContractId(jwt, contractId, parties: Set[Party])(lc)
          .map(_.leftMap { err =>
            unauthorized(
              s"Unauthorized access for fetching contract with id $contractId for parties $parties: $err"
            )
          })
          .map(_.flatMap {
            case GetEventsByContractIdResponse(_, Some(_)) =>
              logger.debug(s"Contract archived for contract id $contractId")
              \/-(Option.empty)
            case GetEventsByContractIdResponse(Some(created), None)
                if created.createdEvent.nonEmpty =>
              ActiveContract
                .fromLedgerApi(
                  ActiveContract.ExtractAs.Template,
                  created.createdEvent.getOrElse(
                    throw new RuntimeException("unreachable since created.createdEvent is nonEmpty")
                  ),
                )
                .leftMap(_.shows)
                .flatMap(apiAcToLfAc(_).leftMap(_.shows))
                .flatMap(_.traverse(lfValueToJsValue(_).leftMap(_.shows)))
                .leftMap { err =>
                  serverError(s"Error processing create event for active contract: $err")
                }
                .map(Some(_))
            case GetEventsByContractIdResponse(_, None) =>
              logger.debug(s"Contract with id $contractId not found")
              \/-(Option.empty)
          })
      )
    )

  private def serverError(
      errorMessage: String
  )(implicit lc: LoggingContextOf[InstanceUUID with RequestID]): EndpointsCompanion.Error = {
    logger.error(s"$errorMessage, ${lc.makeString}")
    EndpointsCompanion.ServerError(new RuntimeException(errorMessage))
  }

  private def invalidUserInput(
      message: String
  )(implicit lc: LoggingContextOf[InstanceUUID with RequestID]): EndpointsCompanion.Error = {
    logger.info(s"$message, ${lc.makeString}")
    EndpointsCompanion.InvalidUserInput(message)
  }

  private def unauthorized(
      message: String
  )(implicit lc: LoggingContextOf[InstanceUUID with RequestID]): EndpointsCompanion.Error = {
    logger.info(s"$message, ${lc.makeString}")
    EndpointsCompanion.Unauthorized(message)
  }

  private def _resolveContractTypeId[U, R[T] <: ContractTypeId[T]](
      jwt: Jwt,
      templateId: U with ContractTypeId.RequiredPkg,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      overload: Overload[U, R],
  ): ET[ContractTypeRef[R]] =
    eitherT(
      resolveContractTypeId[U, R](jwt)(templateId)
        .map(_.leftMap {
          case PackageService.InputError(message) =>
            invalidUserInput(
              s"Invalid user input detected when resolving contract type id for templateId $templateId: $message"
            )
          case PackageService.ServerError(message) =>
            serverError(
              s"Error encountered when resolving contract type id for templateId $templateId: $message"
            )
        }.flatMap(_.toRightDisjunction {
          invalidUserInput(s"Template for id $templateId not found")
        }))
    )

  private def timedETFuture[R](timer: MetricHandle.Timer)(f: ET[R]): ET[R] =
    EitherT.eitherT(Timed.future(timer, f.run))

  private[this] def search: Search = SearchInMemory

  private object SearchInMemory extends Search {
    type LfV = LfValue
    override val lfvToJsValue = SearchValueFormat(lfValueToJsValue)

    override def search(ctx: SearchContext.QueryLang)(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: HttpApiMetrics,
    ) = {
      import ctx.{jwt, parties, templateIds}
      searchInMemory(
        jwt,
        parties,
        templateIds,
      )
    }
  }

  def retrieveAll(
      jwt: Jwt,
      jwtPayload: JwtPayload,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): SearchResult[Error \/ ActiveContract.ResolvedCtTyId[LfValue]] =
    OkResponse(
      Source
        .future(allTemplateIds(lc)(jwt))
        .flatMapConcat(
          Source(_).flatMapConcat(templateId =>
            searchInMemory(jwt, jwtPayload.parties, ResolvedQuery(templateId))
          )
        )
    )

  def search(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      request: GetActiveContractsRequest,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): Future[SearchResult[Error \/ ActiveContract.ResolvedCtTyId[JsValue]]] =
    search(
      jwt,
      request.readAs.cata((_.toSet1), jwtPayload.parties),
      request.templateIds,
    )

  def search(
      jwt: Jwt,
      parties: PartySet,
      templateIds: NonEmpty[Set[ContractTypeId.RequiredPkg]],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): Future[SearchResult[Error \/ ActiveContract.ResolvedCtTyId[JsValue]]] = for {
    res <- resolveContractTypeIds(jwt)(templateIds)
    (resolvedContractTypeIds, unresolvedContractTypeIds) = res

    warnings: Option[UnknownTemplateIds] =
      if (unresolvedContractTypeIds.isEmpty) None
      else Some(UnknownTemplateIds(unresolvedContractTypeIds.toList))
  } yield {
    ResolvedQuery(resolvedContractTypeIds)
      .leftMap(handleResolvedQueryErrors(warnings))
      .map { resolvedQuery =>
        val searchCtx = SearchContext(jwt, parties, resolvedQuery)
        val source = search.toFinal.search(searchCtx)
        OkResponse(source, warnings)
      }
      .merge
  }

  private def handleResolvedQueryErrors(
      warnings: Option[UnknownTemplateIds]
  ): ResolvedQuery.Unsupported => ErrorResponse = unsuppoerted =>
    mkErrorResponse(unsuppoerted.errorMsg, warnings)

  private def mkErrorResponse(errorMessage: String, warnings: Option[UnknownTemplateIds]) =
    ErrorResponse(
      errors = List(errorMessage),
      warnings = warnings,
      status = StatusCodes.BadRequest,
    )

  private[this] def searchInMemory(
      jwt: Jwt,
      parties: PartySet,
      resolvedQuery: ResolvedQuery,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[InternalError \/ ActiveContract.ResolvedCtTyId[LfValue], NotUsed] = {
    logger.debug(
      s"Searching in memory, parties: $parties, resolvedQuery: $resolvedQuery"
    )

    type Ac = ActiveContract.ResolvedCtTyId[LfValue]
    val empty = (Vector.empty[Error], Vector.empty[Ac])
    import InsertDeleteStep.appendForgettingDeletes

    insertDeleteStepSource(jwt, buildTransactionFilter(parties, resolvedQuery))
      .map { step =>
        step.toInsertDelete.partitionMapPreservingIds { apiEvent =>
          ActiveContract
            .fromLedgerApi(ActiveContract.ExtractAs(resolvedQuery), apiEvent)
            .leftMap(e => InternalError(Symbol("searchInMemory"), e.shows))
            .flatMap(apiAcToLfAc): Error \/ Ac
        }
      }
      .fold(empty) { case ((errL, stepL), (errR, stepR)) =>
        (errL ++ errR, appendForgettingDeletes(stepL, stepR))
      }
      .mapConcat { case (err, inserts) =>
        inserts.map(\/-(_)) ++ err.map(-\/(_))
      }
  }

  def liveAcsAsInsertDeleteStepSource(
      jwt: Jwt,
      txnFilter: TransactionFilter,
  )(implicit lc: LoggingContextOf[InstanceUUID]): Source[ContractStreamStep.LAV1, NotUsed] =
    getLedgerEnd(jwt)(lc)
      .flatMapConcat { offset =>
        getActiveContracts(jwt, txnFilter, offset, true)(lc)
          .map { case GetActiveContractsResponse(_, contractEntry) =>
            if (contractEntry.isActiveContract) {
              val createdEvent = contractEntry.activeContract
                .getOrElse(
                  throw new RuntimeException(
                    "unreachable, activeContract should not have been empty since contract is checked to be an active contract"
                  )
                )
                .createdEvent
              Acs(createdEvent.toList.toVector)
            } else LiveBegin(AbsoluteBookmark(Offset(offset)))
          }
          .concat(Source.single(LiveBegin(AbsoluteBookmark(Offset(offset)))))
      }

  /** An ACS ++ transaction stream of `templateIds`, starting at `startOffset` and ending at
    * `terminates`.
    */
  def insertDeleteStepSource(
      jwt: Jwt,
      txnFilter: TransactionFilter,
      startOffset: Option[StartingOffset] = None,
      terminates: Terminates = Terminates.AtParticipantEnd,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[ContractStreamStep.LAV1, NotUsed] = {
    def source =
      (getLedgerEnd(jwt)(lc)
        .flatMapConcat(offset =>
          getActiveContracts(jwt, txnFilter, offset, true)(lc)
            .map(Right(_))
            .concat(Source.single(Left(offset)))
        )
        via logTermination(logger, "ACS upstream"))

    val transactionsSince: String => Source[
      lav2.transaction.Transaction,
      NotUsed,
    ] = off =>
      getCreatesAndArchivesSince(
        jwt,
        txnFilter,
        Offset.assertFromStringToLong(off),
        terminates,
      )(lc) via logTermination(logger, "transactions upstream")

    import com.digitalasset.canton.fetchcontracts.AcsTxStreams.{
      acsFollowingAndBoundary,
      transactionsFollowingBoundary,
    }
    import com.digitalasset.canton.fetchcontracts.util.GraphExtensions.*
    val contractsAndBoundary = startOffset
      .cata(
        so =>
          Source
            .single(AbsoluteBookmark(so.offset))
            .viaMat(transactionsFollowingBoundary(transactionsSince, logger).divertToHead)(
              Keep.right
            ),
        source.viaMat(acsFollowingAndBoundary(transactionsSince, logger).divertToHead)(
          Keep.right
        ),
      )
      .via(logTermination(logger, "ACS+tx or tx stream"))
    contractsAndBoundary mapMaterializedValue { fob =>
      fob.foreach(a =>
        logger.debug(s"contracts fetch completed at: ${a.toString}, ${lc.makeString}")
      )
      NotUsed
    }
  }

  private def apiAcToLfAc(
      ac: ActiveContract.ResolvedCtTyId[ApiValue]
  ): Error \/ ActiveContract.ResolvedCtTyId[LfValue] =
    ac.traverse(ApiValueToLfValueConverter.apiValueToLfValue)
      .leftMap(e => InternalError(Symbol("apiAcToLfAc"), e.shows))

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.attempt(LfValueCodec.apiValueToJsValue(a))(e =>
      InternalError(Symbol("lfValueToJsValue"), e.description)
    )

  def resolveContractTypeIds[Tid <: ContractTypeId.RequiredPkg](
      jwt: Jwt
  )(
      xs: NonEmpty[Set[Tid]]
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[(Set[ContractTypeRef.Resolved], Set[Tid])] = {
    import scalaz.syntax.traverse.*
    import scalaz.std.list.*, scalaz.std.scalaFuture.*

    xs.toList.toNEF
      .traverse { x =>
        resolveContractTypeId(jwt)(x)
          .map(_.toOption.flatten.toLeft(x)): Future[
          Either[ContractTypeRef.Resolved, Tid]
        ]
      }
      .map(_.toSet.partitionMap(a => a))
  }
}

object ContractsService {
  private type ApiValue = lav2.value.Value

  private type LfValue = lf.value.Value

  private final case class SearchValueFormat[-T](encode: T => (Error \/ JsValue))

  private final case class SearchContext[+TpIds](
      jwt: Jwt,
      parties: PartySet,
      templateIds: TpIds,
  )

  private object SearchContext {

    type QueryLang = SearchContext[
      ResolvedQuery
    ]
    type ById = SearchContext[Option[ContractTypeId.RequiredPkg]]
    type Key = SearchContext[ContractTypeId.Template.RequiredPkg]
  }

  // A prototypical abstraction over the in-memory/in-DB split, accounting for
  // the fact that in-memory works with ADT-encoded LF values,
  // whereas in-DB works with JsValues
  private sealed abstract class Search { self =>
    type LfV
    val lfvToJsValue: SearchValueFormat[LfV]

    final def toFinal: Search { type LfV = JsValue } = {
      val SearchValueFormat(convert) = lfvToJsValue
      new Search {
        type LfV = JsValue
        override val lfvToJsValue = SearchValueFormat(\/.right)

        override def search(
            ctx: SearchContext.QueryLang
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID],
            metrics: HttpApiMetrics,
        ): Source[Error \/ ActiveContract.ResolvedCtTyId[LfV], NotUsed] =
          self.search(ctx) map (_ flatMap (_ traverse convert))
      }
    }

    def search(ctx: SearchContext.QueryLang)(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: HttpApiMetrics,
    ): Source[Error \/ ActiveContract.ResolvedCtTyId[LfV], NotUsed]
  }

  final case class Error(id: Symbol, message: String)
  private type InternalError = Error
  val InternalError: Error.type = Error

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"ContractService Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }

  type SearchResult[A] = SyncResponse[Source[A, NotUsed]]

  def buildTransactionFilter(
      parties: PartySet,
      resolvedQuery: ResolvedQuery,
  ): TransactionFilter =
    transactionFilter(parties, resolvedQuery.resolved.map(_.original).toList)
}
