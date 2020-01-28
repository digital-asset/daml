// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.stream.scaladsl._
import akka.stream.Materializer
import com.digitalasset.daml.lf
import com.digitalasset.http.ContractsFetch.InsertDeleteStep
import com.digitalasset.http.LedgerClientJwt.Terminates
import com.digitalasset.http.dbbackend.ContractDao
import com.digitalasset.http.domain.{GetActiveContractsRequest, JwtPayload, TemplateId}
import com.digitalasset.http.json.JsonProtocol.LfValueCodec
import com.digitalasset.http.query.ValuePredicate
import com.digitalasset.http.util.ApiValueToLfValueConverter
import com.digitalasset.http.util.FutureUtil.toFuture
import util.Collections._
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => api}
import com.digitalasset.util.ExceptionOps._
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.{-\/, Show, \/, \/-}
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

      predicate = isContractKey(contractKey) _

      errorOrAc <- searchInMemoryOneTpId(jwt, party, resolvedTemplateId, Map.empty)
        .collect {
          case e @ -\/(_) => e
          case a @ \/-(ac) if predicate(ac) => a
        }
        .runWith(Sink.headOption): Future[Option[Error \/ domain.ActiveContract[LfValue]]]

      result <- lookupResult(errorOrAc)

    } yield result

  private def isContractKey(k: LfValue)(a: domain.ActiveContract[LfValue]): Boolean =
    a.key.fold(false)(_ == k)

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
    SearchResult(
      Source(allTemplateIds()).flatMapConcat(x => searchInMemoryOneTpId(jwt, party, x, Map.empty)),
      Set.empty,
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
      templateIds: Set[domain.TemplateId.OptionalPkg],
      queryParams: Map[String, JsValue],
  ): SearchResult[Error \/ domain.ActiveContract[JsValue]] = {

    val (resolvedTemplateIds, unresolvedTemplateIds) = resolveTemplateIds(templateIds)

    val source = daoAndFetch.cata(
      x => searchDb(x._1, x._2)(jwt, party, resolvedTemplateIds, queryParams),
      searchInMemory(jwt, party, resolvedTemplateIds, queryParams)
        .map(_.flatMap(lfAcToJsAc)),
    )

    SearchResult(source, unresolvedTemplateIds)
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
        val (errors, inserts) = step.inserts partitionMap { apiEvent =>
          domain.ActiveContract
            .fromLedgerApi(apiEvent)
            .leftMap(e => Error('searchInMemory, e.shows))
            .flatMap(apiAcToLfAc): Error \/ Ac
        }
        (
          errors,
          step copy (inserts = inserts filter (ac => funPredicates(ac.templateId)(ac.payload))),
        )
      }
      .fold(empty) {
        case ((errL, stepL), (errR, stepR)) =>
          (errL ++ errR, appendForgettingDeletes(stepL, stepR)(_.contractId.unwrap))
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

  private[http] def insertDeleteStepSource(
      jwt: Jwt,
      party: lar.Party,
      templateIds: List[domain.TemplateId.RequiredPkg],
      terminates: Terminates = Terminates.AtLedgerEnd,
  ): Source[InsertDeleteStep[api.event.CreatedEvent], NotUsed] = {

    val txnFilter = util.Transactions.transactionFilterFor(party, templateIds)
    val source = getActiveContracts(jwt, txnFilter, true)

    val transactionsSince
        : api.ledger_offset.LedgerOffset => Source[api.transaction.Transaction, NotUsed] =
      getCreatesAndArchivesSince(jwt, txnFilter, _: api.ledger_offset.LedgerOffset, terminates)

    import ContractsFetch.acsFollowingAndBoundary, ContractsFetch.GraphExtensions._
    val contractsAndBoundary = acsFollowingAndBoundary(transactionsSince).divertToHead
    source
      .viaMat(contractsAndBoundary) { (nu, fob) =>
        fob.foreach(a => logger.debug(s"contracts fetch completed at: ${a.toString}"))
        nu
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
      Error('lfValueToJsValue, e.description),
    )

  private def resolveTemplateIds(
      xs: Set[domain.TemplateId.OptionalPkg],
  ): (Set[domain.TemplateId.RequiredPkg], Set[domain.TemplateId.OptionalPkg]) = {
    xs.partitionMap { x =>
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

  final case class SearchResult[A](
      source: Source[A, NotUsed],
      unresolvedTemplateIds: Set[domain.TemplateId.OptionalPkg],
  )
}
