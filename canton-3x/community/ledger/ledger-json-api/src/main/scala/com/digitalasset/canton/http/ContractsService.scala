// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.stream.scaladsl.*
import com.daml.lf
import com.digitalasset.canton.http.LedgerClientJwt.Terminates
import com.digitalasset.canton.http.json.JsonProtocol.LfValueCodec
import com.digitalasset.canton.http.domain.{
  ActiveContract,
  ContractTypeId,
  GetActiveContractsRequest,
  JwtPayload,
}
import util.ApiValueToLfValueConverter
import com.digitalasset.canton.fetchcontracts.AcsTxStreams.{transactionFilter, transactionFilterV1}
import com.digitalasset.canton.fetchcontracts.util.ContractStreamStep.{Acs, LiveBegin}
import com.digitalasset.canton.fetchcontracts.util.GraphExtensions.*
import com.digitalasset.canton.http.Endpoints.ET
import com.digitalasset.canton.http.PackageService.ResolveContractTypeId.Overload
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.http.util.FutureUtil.{either, eitherT}
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v1 as api
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle
import com.daml.nonempty.NonEmpty
import com.daml.scalautil.ExceptionOps.*
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.fetchcontracts.util.{
  AbsoluteBookmark,
  ContractStreamStep,
  InsertDeleteStep,
}
import com.digitalasset.canton.http.EndpointsCompanion.NotFound
import scalaz.syntax.show.*
import scalaz.syntax.std.option.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, EitherT, Show, \/, \/-}
import spray.json.JsValue

import scala.concurrent.{ExecutionContext, Future}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import scalaz.std.scalaFuture.*

class ContractsService(
    resolveContractTypeId: PackageService.ResolveContractTypeId,
    allTemplateIds: PackageService.AllTemplateIds,
    getContractByContractId: LedgerClientJwt.GetContractByContractId,
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with NoTracing {
  import ContractsService.*

  private type ActiveContractO = Option[domain.ActiveContract.ResolvedCtTyId[JsValue]]

  def resolveContractReference(
      jwt: Jwt,
      parties: domain.PartySet,
      contractLocator: domain.ContractLocator[LfValue],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[domain.ResolvedContractRef[LfValue]] = {
    import Overload.Template
    contractLocator match {
      case domain.EnrichedContractKey(templateId, key) =>
        _resolveContractTypeId(jwt, templateId).map(x => -\/(x -> key))
      case domain.EnrichedContractId(Some(templateId), contractId) =>
        _resolveContractTypeId(jwt, templateId).map(x => \/-(x -> contractId))
      case domain.EnrichedContractId(None, contractId) =>
        findByContractId(jwt, parties, contractId)
          .flatMap {
            case Some(value) =>
              EitherT.pure(value): ET[domain.ActiveContract.ResolvedCtTyId[JsValue]]
            case None =>
              EitherT.pureLeft(
                invalidUserInput(
                  s"Could not resolve contract reference for contract id $contractId"
                )
              ): ET[
                domain.ActiveContract.ResolvedCtTyId[JsValue]
              ]
          }
          .map(a => \/-(a.templateId -> a.contractId))
    }
  }

  def lookup(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      req: domain.FetchRequest[LfValue],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[ActiveContractO] = {
    val readAs = req.readAs.cata(_.toSet1, jwtPayload.parties)
    req.locator match {
      case domain.EnrichedContractKey(_templateId, _contractKey) =>
        either(-\/(NotFound("lookup by contract key is not supported")))
      //  TODO(#16065)
      //  findByContractKey(jwt, readAs, templateId, contractKey)
      case domain.EnrichedContractId(_templateId, contractId) =>
        findByContractId(jwt, readAs, contractId)
    }
  }

//  TODO(#16065)
//  private[this] def findByContractKey(
//      jwt: Jwt,
//      parties: domain.PartySet,
//      templateId: ContractTypeId.Template.OptionalPkg,
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
//                .fromLedgerApi(domain.ActiveContract.IgnoreInterface, createdEvent)
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
      parties: domain.PartySet,
      contractId: domain.ContractId,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[ActiveContractO] =
    timedETFuture(metrics.dbFindByContractId)(
      eitherT(
        getContractByContractId(jwt, contractId, parties: Set[domain.Party])(lc)
          .map(_.leftMap { err =>
            unauthorized(
              s"Unauthorized access for fetching contract with id $contractId for parties $parties: $err"
            )
          })
          .map(_.flatMap {
            case GetEventsByContractIdResponse(_, Some(_), _) =>
              logger.debug(s"Contract archived for contract id $contractId")
              \/-(Option.empty)
            case GetEventsByContractIdResponse(Some(created), None, _)
                if created.createdEvent.nonEmpty =>
              ActiveContract
                .fromLedgerApi(
                  domain.ActiveContract.IgnoreInterface,
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
            case GetEventsByContractIdResponse(_, None, _) =>
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

  private def _resolveContractTypeId[U, R](
      jwt: Jwt,
      templateId: U with ContractTypeId.OptionalPkg,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      overload: Overload[U, R],
  ): ET[R] =
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
  ): SearchResult[Error \/ domain.ActiveContract.ResolvedCtTyId[LfValue]] = {
    domain.OkResponse(
      Source
        .future(allTemplateIds(lc)(jwt))
        .flatMapConcat(
          Source(_).flatMapConcat(templateId =>
            searchInMemory(jwt, jwtPayload.parties, domain.ResolvedQuery(templateId))
          )
        )
    )
  }

  def search(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      request: GetActiveContractsRequest,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): Future[SearchResult[Error \/ domain.ActiveContract.ResolvedCtTyId[JsValue]]] =
    search(
      jwt,
      request.readAs.cata((_.toSet1), jwtPayload.parties),
      request.templateIds,
    )

  def search(
      jwt: Jwt,
      parties: domain.PartySet,
      templateIds: NonEmpty[Set[domain.ContractTypeId.OptionalPkg]],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): Future[SearchResult[Error \/ domain.ActiveContract.ResolvedCtTyId[JsValue]]] = for {
    res <- resolveContractTypeIds(jwt)(templateIds)
    (resolvedContractTypeIds, unresolvedContractTypeIds) = res

    warnings: Option[domain.UnknownTemplateIds] =
      if (unresolvedContractTypeIds.isEmpty) None
      else Some(domain.UnknownTemplateIds(unresolvedContractTypeIds.toList))
  } yield {
    domain
      .ResolvedQuery(resolvedContractTypeIds)
      .leftMap(handleResolvedQueryErrors(warnings))
      .map { resolvedQuery =>
        val searchCtx = SearchContext(jwt, parties, resolvedQuery)
        val source = search.toFinal.search(searchCtx)
        domain.OkResponse(source, warnings)
      }
      .merge
  }

  private def handleResolvedQueryErrors(
      warnings: Option[domain.UnknownTemplateIds]
  ): domain.ResolvedQuery.Unsupported => domain.ErrorResponse = unsuppoerted =>
    mkErrorResponse(unsuppoerted.errorMsg, warnings)

  private def mkErrorResponse(errorMessage: String, warnings: Option[domain.UnknownTemplateIds]) =
    domain.ErrorResponse(
      errors = List(errorMessage),
      warnings = warnings,
      status = StatusCodes.BadRequest,
    )

  private[this] def searchInMemory(
      jwt: Jwt,
      parties: domain.PartySet,
      resolvedQuery: domain.ResolvedQuery,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[InternalError \/ domain.ActiveContract.ResolvedCtTyId[LfValue], NotUsed] = {
    val templateIds = resolvedQuery.resolved
    logger.debug(
      s"Searching in memory, parties: $parties, templateIds: $templateIds"
    )

    type Ac = domain.ActiveContract.ResolvedCtTyId[LfValue]
    val empty = (Vector.empty[Error], Vector.empty[Ac])
    import InsertDeleteStep.appendForgettingDeletes

    insertDeleteStepSource(jwt, parties, templateIds.toList)
      .map { step =>
        step.toInsertDelete.partitionMapPreservingIds { apiEvent =>
          domain.ActiveContract
            .fromLedgerApi(resolvedQuery, apiEvent)
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
      parties: domain.PartySet,
      templateIds: List[domain.ContractTypeId.Resolved],
  )(implicit lc: LoggingContextOf[InstanceUUID]): Source[ContractStreamStep.LAV1, NotUsed] = {
    val txnFilter = transactionFilter(parties, templateIds)
    getActiveContracts(jwt, txnFilter, true)(lc)
      .map { case GetActiveContractsResponse(offset, _, contractEntry, _) =>
        if (contractEntry.isActiveContract) {
          val createdEvent = contractEntry.activeContract
            .getOrElse(
              throw new RuntimeException(
                "unreachable, activeContract should not have been empty since contract is checked to be an active contract"
              )
            )
            .createdEvent
          Acs(createdEvent.toVector)
        } else LiveBegin(AbsoluteBookmark(domain.Offset(offset)))
      }
  }

  /** An ACS ++ transaction stream of `templateIds`, starting at `startOffset`
    * and ending at `terminates`.
    */
  def insertDeleteStepSource(
      jwt: Jwt,
      parties: domain.PartySet,
      templateIds: List[domain.ContractTypeId.Resolved],
      startOffset: Option[domain.StartingOffset] = None,
      terminates: Terminates = Terminates.AtLedgerEnd,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[ContractStreamStep.LAV1, NotUsed] = {

    val txnFilter = transactionFilter(parties, templateIds)
    val txnFilterV1 = transactionFilterV1(parties, templateIds)
    def source =
      (getActiveContracts(jwt, txnFilter, true)(lc)
        via logTermination(logger, "ACS upstream"))

    val transactionsSince
        : api.ledger_offset.LedgerOffset => Source[api.transaction.Transaction, NotUsed] =
      getCreatesAndArchivesSince(
        jwt,
        txnFilterV1,
        _: api.ledger_offset.LedgerOffset,
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
      ac: domain.ActiveContract.ResolvedCtTyId[ApiValue]
  ): Error \/ domain.ActiveContract.ResolvedCtTyId[LfValue] =
    ac.traverse(ApiValueToLfValueConverter.apiValueToLfValue)
      .leftMap(e => InternalError(Symbol("apiAcToLfAc"), e.shows))

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.attempt(LfValueCodec.apiValueToJsValue(a))(e =>
      InternalError(Symbol("lfValueToJsValue"), e.description)
    )

  def resolveContractTypeIds[Tid <: domain.ContractTypeId.OptionalPkg](
      jwt: Jwt
  )(
      xs: NonEmpty[Set[Tid]]
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[(Set[domain.ContractTypeId.Resolved], Set[Tid])] = {
    import scalaz.syntax.traverse.*
    import scalaz.std.list.*, scalaz.std.scalaFuture.*

    xs.toList.toNEF
      .traverse { x =>
        resolveContractTypeId(jwt)(x)
          .map(_.toOption.flatten.toLeft(x)): Future[
          Either[domain.ContractTypeId.Resolved, Tid]
        ]
      }
      .map(_.toSet.partitionMap(a => a))
  }
}

object ContractsService {
  private type ApiValue = api.value.Value

  private type LfValue = lf.value.Value

  private final case class SearchValueFormat[-T](encode: T => (Error \/ JsValue))

  private final case class SearchContext[+TpIds](
      jwt: Jwt,
      parties: domain.PartySet,
      templateIds: TpIds,
  )

  private object SearchContext {

    type QueryLang = SearchContext[
      domain.ResolvedQuery
    ]
    type ById = SearchContext[Option[domain.ContractTypeId.OptionalPkg]]
    type Key = SearchContext[domain.ContractTypeId.Template.OptionalPkg]
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
        ): Source[Error \/ domain.ActiveContract.ResolvedCtTyId[LfV], NotUsed] =
          self.search(ctx) map (_ flatMap (_ traverse convert))
      }
    }

    def search(ctx: SearchContext.QueryLang)(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: HttpApiMetrics,
    ): Source[Error \/ domain.ActiveContract.ResolvedCtTyId[LfV], NotUsed]
  }

  final case class Error(id: Symbol, message: String)
  private type InternalError = Error
  val InternalError: Error.type = Error

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"ContractService Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }

  type SearchResult[A] = domain.SyncResponse[Source[A, NotUsed]]
}
