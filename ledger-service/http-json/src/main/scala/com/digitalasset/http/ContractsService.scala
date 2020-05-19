// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl._
import akka.stream.Materializer
import com.daml.lf
import com.daml.http.LedgerClientJwt.Terminates
import com.daml.http.dbbackend.ContractDao
import com.daml.http.domain.{GetActiveContractsRequest, JwtPayload, TemplateId}
import com.daml.http.json.JsonProtocol.LfValueCodec
import com.daml.http.query.ValuePredicate
import com.daml.http.util.ApiValueToLfValueConverter
import com.daml.http.util.FutureUtil.toFuture
import util.Collections._
import util.{ContractStreamStep, InsertDeleteStep}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.{v1 => api}
import com.daml.util.ExceptionOps._
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, OneAnd, Show, Tag, \/, \/-}
import spray.json.JsValue

import scala.collection.breakOut
import scala.concurrent.{ExecutionContext, Future}

// TODO(Leo) split it into ContractsServiceInMemory and ContractsServiceDb
class ContractsService(
    resolveTemplateId: PackageService.ResolveTemplateId,
    allTemplateIds: PackageService.AllTemplateIds,
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    getTermination: LedgerClientJwt.GetTermination,
    lookupType: query.ValuePredicate.TypeLookup,
    contractDao: Option[dbbackend.ContractDao],
    parallelism: Int = 8,
)(implicit ec: ExecutionContext, mat: Materializer)
    extends StrictLogging {

  import ContractsService._

  type CompiledPredicates = Map[domain.TemplateId.RequiredPkg, query.ValuePredicate]

  private val daoAndFetch: Option[(dbbackend.ContractDao, ContractsFetch)] = contractDao.map {
    dao =>
      (
        dao,
        new ContractsFetch(
          getActiveContracts,
          getCreatesAndArchivesSince,
          getTermination,
          lookupType,
        )(
          dao.logHandler,
        ),
      )
  }

  def resolveContractReference(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      contractLocator: domain.ContractLocator[LfValue],
  ): Future[Option[domain.ResolvedContractRef[LfValue]]] =
    contractLocator match {
      case domain.EnrichedContractKey(templateId, key) =>
        Future.successful(resolveTemplateId(templateId).map(x => -\/(x -> key)))
      case domain.EnrichedContractId(Some(templateId), contractId) =>
        Future.successful(resolveTemplateId(templateId).map(x => \/-(x -> contractId)))
      case domain.EnrichedContractId(None, contractId) =>
        findByContractId(jwt, jwtPayload.party, None, contractId)
          .map(_.map(a => \/-(a.templateId -> a.contractId)))
    }

  def lookup(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      contractLocator: domain.ContractLocator[LfValue],
  ): Future[Option[domain.ActiveContract[LfValue]]] =
    contractLocator match {
      case domain.EnrichedContractKey(templateId, contractKey) =>
        findByContractKey(jwt, jwtPayload.party, templateId, contractKey)
      case domain.EnrichedContractId(templateId, contractId) =>
        findByContractId(jwt, jwtPayload.party, templateId, contractId)
    }

  def findByContractKey(
      jwt: Jwt,
      party: lar.Party,
      templateId: TemplateId.OptionalPkg,
      contractKey: LfValue,
  ): Future[Option[domain.ActiveContract[LfValue]]] =
    for {

      resolvedTemplateId <- toFuture(resolveTemplateId(templateId)): Future[TemplateId.RequiredPkg]

      predicate = domain.ActiveContract.matchesKey(contractKey) _

      errorOrAc <- searchInMemoryOneTpId(jwt, party, resolvedTemplateId, Map.empty)
        .collect {
          case e @ -\/(_) => e
          case a @ \/-(ac) if predicate(ac) => a
        }
        .runWith(Sink.headOption): Future[Option[Error \/ domain.ActiveContract[LfValue]]]

      result <- lookupResult(errorOrAc)

    } yield result

  def findByContractId(
      jwt: Jwt,
      party: lar.Party,
      templateId: Option[domain.TemplateId.OptionalPkg],
      contractId: domain.ContractId,
  ): Future[Option[domain.ActiveContract[LfValue]]] =
    for {

      resolvedTemplateIds <- templateId.cata(
        x => toFuture(resolveTemplateId(x).map(Set(_))),
        Future.successful(allTemplateIds()),
      ): Future[Set[domain.TemplateId.RequiredPkg]]

      errorOrAc <- searchInMemory(jwt, party, resolvedTemplateIds, Map.empty)
        .collect {
          case e @ -\/(_) => e
          case a @ \/-(ac) if isContractId(contractId)(ac) => a
        }
        .runWith(Sink.headOption): Future[Option[Error \/ domain.ActiveContract[LfValue]]]

      result <- lookupResult(errorOrAc)

    } yield result

  private def lookupResult(
      errorOrAc: Option[Error \/ domain.ActiveContract[LfValue]],
  ): Future[Option[domain.ActiveContract[LfValue]]] = {
    errorOrAc.cata(x => toFuture(x).map(Some(_)), Future.successful(None))
  }

  private def isContractId(k: domain.ContractId)(a: domain.ActiveContract[LfValue]): Boolean =
    (a.contractId: domain.ContractId) == k

  def retrieveAll(
      jwt: Jwt,
      jwtPayload: JwtPayload,
  ): SearchResult[Error \/ domain.ActiveContract[LfValue]] =
    retrieveAll(jwt, jwtPayload.party)

  def retrieveAll(
      jwt: Jwt,
      party: domain.Party,
  ): SearchResult[Error \/ domain.ActiveContract[LfValue]] =
    domain.OkResponse(
      Source(allTemplateIds()).flatMapConcat(x => searchInMemoryOneTpId(jwt, party, x, Map.empty))
    )

  def search(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      request: GetActiveContractsRequest,
  ): SearchResult[Error \/ domain.ActiveContract[JsValue]] =
    search(jwt, jwtPayload.party, request.templateIds, request.query)

  def search(
      jwt: Jwt,
      party: domain.Party,
      templateIds: OneAnd[Set, domain.TemplateId.OptionalPkg],
      queryParams: Map[String, JsValue],
  ): SearchResult[Error \/ domain.ActiveContract[JsValue]] = {

    val (resolvedTemplateIds, unresolvedTemplateIds) = resolveTemplateIds(templateIds)

    val warnings: Option[domain.UnknownTemplateIds] =
      if (unresolvedTemplateIds.isEmpty) None
      else Some(domain.UnknownTemplateIds(unresolvedTemplateIds.toList))

    if (resolvedTemplateIds.isEmpty) {
      domain.ErrorResponse(
        errors = List(ErrorMessages.cannotResolveAnyTemplateId),
        warnings = warnings,
        status = StatusCodes.BadRequest
      )
    } else {
      val source = daoAndFetch.cata(
        x => searchDb(x._1, x._2)(jwt, party, resolvedTemplateIds, queryParams),
        searchInMemory(jwt, party, resolvedTemplateIds, queryParams)
          .map(_.flatMap(lfAcToJsAc)),
      )
      domain.OkResponse(source, warnings)
    }
  }

  // we store create arguments as JSON in DB, that is why it is `domain.ActiveContract[JsValue]` in the result
  def searchDb(dao: dbbackend.ContractDao, fetch: ContractsFetch)(
      jwt: Jwt,
      party: domain.Party,
      templateIds: Set[domain.TemplateId.RequiredPkg],
      queryParams: Map[String, JsValue],
  ): Source[Error \/ domain.ActiveContract[JsValue], NotUsed] = {

    // TODO use `stream` when materializing DBContracts, so we could stream ActiveContracts
    val fv: Future[Vector[domain.ActiveContract[JsValue]]] = dao
      .transact { searchDb_(fetch, dao.logHandler)(jwt, party, templateIds, queryParams) }
      .unsafeToFuture()

    Source.future(fv).mapConcat(identity).map(\/.right)
  }

  private def searchDb_(fetch: ContractsFetch, doobieLog: doobie.LogHandler)(
      jwt: Jwt,
      party: domain.Party,
      templateIds: Set[domain.TemplateId.RequiredPkg],
      queryParams: Map[String, JsValue],
  ): doobie.ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
    import cats.instances.vector._
    import cats.syntax.traverse._
    import doobie.implicits._
    for {
      _ <- fetch.fetchAndPersist(jwt, party, templateIds.toList)
      cts <- templateIds.toVector
        .traverse(tpId => searchDbOneTpId_(fetch, doobieLog)(jwt, party, tpId, queryParams))
    } yield cts.flatten
  }

  private[this] def searchDbOneTpId_(fetch: ContractsFetch, doobieLog: doobie.LogHandler)(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg,
      queryParams: Map[String, JsValue],
  ): doobie.ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
    val predicate = valuePredicate(templateId, queryParams)
    ContractDao.selectContracts(party, templateId, predicate.toSqlWhereClause)(doobieLog)
  }

  private def searchInMemory(
      jwt: Jwt,
      party: domain.Party,
      templateIds: Set[domain.TemplateId.RequiredPkg],
      queryParams: Map[String, JsValue],
  ): Source[Error \/ domain.ActiveContract[LfValue], NotUsed] = {

    type Ac = domain.ActiveContract[LfValue]
    val empty = (Vector.empty[Error], Vector.empty[Ac])
    import InsertDeleteStep.appendForgettingDeletes

    val funPredicates: Map[domain.TemplateId.RequiredPkg, LfValue => Boolean] =
      templateIds.map(tid => (tid, valuePredicate(tid, queryParams).toFunPredicate))(breakOut)

    insertDeleteStepSource(jwt, party, templateIds.toList)
      .map { step =>
        val (errors, converted) = step.toInsertDelete.partitionMapPreservingIds { apiEvent =>
          domain.ActiveContract
            .fromLedgerApi(apiEvent)
            .leftMap(e => Error('searchInMemory, e.shows))
            .flatMap(apiAcToLfAc): Error \/ Ac
        }
        val convertedInserts = converted.inserts filter { ac =>
          funPredicates.get(ac.templateId).exists(_(ac.payload))
        }
        (errors, converted.copy(inserts = convertedInserts))
      }
      .fold(empty) {
        case ((errL, stepL), (errR, stepR)) =>
          (errL ++ errR, appendForgettingDeletes(stepL, stepR))
      }
      .mapConcat {
        case (err, inserts) =>
          inserts.map(\/-(_)) ++ err.map(-\/(_))
      }
  }

  private def searchInMemoryOneTpId(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg,
      queryParams: Map[String, JsValue],
  ): Source[Error \/ domain.ActiveContract[LfValue], NotUsed] =
    searchInMemory(jwt, party, Set(templateId), queryParams)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private[http] def insertDeleteStepSource(
      jwt: Jwt,
      party: lar.Party,
      templateIds: List[domain.TemplateId.RequiredPkg],
      startOffset: Option[domain.StartingOffset] = None,
      terminates: Terminates = Terminates.AtLedgerEnd,
  ): Source[ContractStreamStep.LAV1, NotUsed] = {

    val txnFilter = util.Transactions.transactionFilterFor(party, templateIds)
    def source = getActiveContracts(jwt, txnFilter, true)

    val transactionsSince
      : api.ledger_offset.LedgerOffset => Source[api.transaction.Transaction, NotUsed] =
      getCreatesAndArchivesSince(jwt, txnFilter, _: api.ledger_offset.LedgerOffset, terminates)

    import ContractsFetch.{acsFollowingAndBoundary, transactionsFollowingBoundary},
    ContractsFetch.GraphExtensions._
    val contractsAndBoundary = startOffset.cata(
      so =>
        Source
          .single(Tag unsubst util.AbsoluteBookmark(so.offset))
          .viaMat(transactionsFollowingBoundary(transactionsSince).divertToHead)(Keep.right),
      source.viaMat(acsFollowingAndBoundary(transactionsSince).divertToHead)(Keep.right)
    )
    contractsAndBoundary mapMaterializedValue { fob =>
      fob.foreach(a => logger.debug(s"contracts fetch completed at: ${a.toString}"))
      NotUsed
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def apiAcToLfAc(
      ac: domain.ActiveContract[ApiValue],
  ): Error \/ domain.ActiveContract[LfValue] =
    ac.traverse(ApiValueToLfValueConverter.apiValueToLfValue)
      .leftMap(e => Error('apiAcToLfAc, e.shows))

  private[http] def valuePredicate(
      templateId: domain.TemplateId.RequiredPkg,
      q: Map[String, JsValue],
  ): query.ValuePredicate =
    ValuePredicate.fromTemplateJsObject(q, templateId, lookupType)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def lfAcToJsAc(
      a: domain.ActiveContract[LfValue],
  ): Error \/ domain.ActiveContract[JsValue] =
    a.traverse(lfValueToJsValue)

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.fromTryCatchNonFatal(LfValueCodec.apiValueToJsValue(a)).leftMap(e =>
      Error('lfValueToJsValue, e.description))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private[http] def resolveTemplateIds[Tid <: domain.TemplateId.OptionalPkg](
      xs: OneAnd[Set, Tid]
  ): (Set[domain.TemplateId.RequiredPkg], Set[Tid]) = {
    import scalaz.std.iterable._
    import scalaz.syntax.foldable._

    xs.toSet.partitionMap { x =>
      resolveTemplateId(x) toLeftDisjunction x
    }
  }
}

object ContractsService {
  private type ApiValue = api.value.Value

  private type LfValue = lf.value.Value[lf.value.Value.AbsoluteContractId]

  case class Error(id: Symbol, message: String)

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"ContractService Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }

  type SearchResult[A] = domain.SyncResponse[Source[A, NotUsed]]
}
