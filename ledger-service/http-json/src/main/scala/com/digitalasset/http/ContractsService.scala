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
import com.daml.http.json.JsonProtocol.LfValueDatabaseCodec.{
  apiValueToJsValue => toDbCompatibleJson
}
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
import scalaz.Id.Id
import scalaz.std.option._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, OneAnd, Show, Tag, \/, \/-}
import spray.json.JsValue

import scala.language.higherKinds
import scala.concurrent.{ExecutionContext, Future}

class ContractsService(
    resolveTemplateId: PackageService.ResolveTemplateId,
    allTemplateIds: PackageService.AllTemplateIds,
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    getTermination: LedgerClientJwt.GetTermination,
    lookupType: query.ValuePredicate.TypeLookup,
    contractDao: Option[dbbackend.ContractDao],
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
        )(
          dao.logHandler,
        ),
      )
  }

  def resolveContractReference(
      jwt: Jwt,
      parties: OneAnd[Set, domain.Party],
      contractLocator: domain.ContractLocator[LfValue],
  ): Future[Option[domain.ResolvedContractRef[LfValue]]] =
    contractLocator match {
      case domain.EnrichedContractKey(templateId, key) =>
        Future.successful(resolveTemplateId(templateId).map(x => -\/(x -> key)))
      case domain.EnrichedContractId(Some(templateId), contractId) =>
        Future.successful(resolveTemplateId(templateId).map(x => \/-(x -> contractId)))
      case domain.EnrichedContractId(None, contractId) =>
        findByContractId(jwt, parties, None, contractId)
          .map(_.map(a => \/-(a.templateId -> a.contractId)))
    }

  def lookup(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      contractLocator: domain.ContractLocator[LfValue],
  ): Future[Option[domain.ActiveContract[JsValue]]] =
    contractLocator match {
      case domain.EnrichedContractKey(templateId, contractKey) =>
        findByContractKey(jwt, jwtPayload.parties, templateId, contractKey)
      case domain.EnrichedContractId(templateId, contractId) =>
        findByContractId(jwt, jwtPayload.parties, templateId, contractId)
    }

  private[this] def findByContractKey(
      jwt: Jwt,
      parties: OneAnd[Set, lar.Party],
      templateId: TemplateId.OptionalPkg,
      contractKey: LfValue,
  ): Future[Option[domain.ActiveContract[JsValue]]] =
    search.toFinal
      .findByContractKey(SearchContext[Id, Option](jwt, parties, templateId), contractKey)

  private[this] def findByContractId(
      jwt: Jwt,
      parties: OneAnd[Set, lar.Party],
      templateId: Option[domain.TemplateId.OptionalPkg],
      contractId: domain.ContractId,
  ): Future[Option[domain.ActiveContract[JsValue]]] =
    search.toFinal.findByContractId(SearchContext(jwt, parties, templateId), contractId)

  private[this] def search: Search = SearchDb getOrElse SearchInMemory

  private object SearchInMemory extends Search {
    type LfV = LfValue
    override val lfvToJsValue = SearchValueFormat(lfValueToJsValue)

    override def findByContractKey(
        ctx: SearchContext[Id, Option],
        contractKey: LfValue,
    ): Future[Option[domain.ActiveContract[LfValue]]] = {
      import ctx.{jwt, parties, templateIds => templateId}
      for {
        resolvedTemplateId <- toFuture(resolveTemplateId(templateId)): Future[
          TemplateId.RequiredPkg]

        predicate = domain.ActiveContract.matchesKey(contractKey) _

        errorOrAc <- searchInMemoryOneTpId(jwt, parties, resolvedTemplateId, predicate)
          .runWith(Sink.headOption): Future[Option[Error \/ domain.ActiveContract[LfValue]]]

        result <- lookupResult(errorOrAc)

      } yield result
    }

    override def findByContractId(
        ctx: SearchContext[Option, Option],
        contractId: domain.ContractId,
    ): Future[Option[domain.ActiveContract[LfValue]]] = {
      import ctx.{jwt, parties, templateIds => templateId}
      for {

        resolvedTemplateIds <- templateId.cata(
          x => toFuture(resolveTemplateId(x).map(Set(_))),
          Future.successful(allTemplateIds()),
        ): Future[Set[domain.TemplateId.RequiredPkg]]

        errorOrAc <- searchInMemory(
          jwt,
          parties,
          resolvedTemplateIds,
          InMemoryQuery.Filter(isContractId(contractId)))
          .runWith(Sink.headOption): Future[Option[Error \/ domain.ActiveContract[LfValue]]]

        result <- lookupResult(errorOrAc)

      } yield result
    }

    override def search(ctx: SearchContext[Set, Id], queryParams: Map[String, JsValue]) = {
      import ctx.{jwt, parties, templateIds}
      searchInMemory(jwt, parties, templateIds, InMemoryQuery.Params(queryParams))
    }
  }

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
    retrieveAll(jwt, jwtPayload.parties)

  def retrieveAll(
      jwt: Jwt,
      parties: OneAnd[Set, domain.Party],
  ): SearchResult[Error \/ domain.ActiveContract[LfValue]] =
    domain.OkResponse(
      Source(allTemplateIds()).flatMapConcat(x => searchInMemoryOneTpId(jwt, parties, x, _ => true))
    )

  def search(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      request: GetActiveContractsRequest,
  ): SearchResult[Error \/ domain.ActiveContract[JsValue]] =
    search(jwt, jwtPayload.parties, request.templateIds, request.query)

  def search(
      jwt: Jwt,
      parties: OneAnd[Set, domain.Party],
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
      val searchCtx = SearchContext[Set, Id](jwt, parties, resolvedTemplateIds)
      val source = search.toFinal.search(searchCtx, queryParams)
      domain.OkResponse(source, warnings)
    }
  }

  private[this] val SearchDb: Option[Search { type LfV = JsValue }] = daoAndFetch map {
    case (dao, fetch) =>
      new Search {
        // we store create arguments as JSON in DB, that is why it is `JsValue` in the result
        type LfV = JsValue
        override val lfvToJsValue = SearchValueFormat(\/.right)

        override def findByContractId(
            ctx: SearchContext[Option, Option],
            contractId: domain.ContractId,
        ): Future[Option[domain.ActiveContract[LfV]]] = {
          import ctx.{jwt, parties, templateIds => otemplateId}
          val dbQueried = for {
            templateId <- otemplateId
            resolved <- resolveTemplateId(templateId)
          } yield
            unsafeRunAsync {
              import dao.logHandler
              import doobie.implicits._, cats.syntax.apply._
              fetch.fetchAndPersist(jwt, parties, List(resolved)) *>
                ContractDao.fetchById(parties, resolved, contractId)
            }
          dbQueried getOrElse {
            // we need a template ID to update the database
            SearchInMemory.toFinal.findByContractId(ctx, contractId)
          }
        }

        override def findByContractKey(
            ctx: SearchContext[Id, Option],
            contractKey: LfValue,
        ): Future[Option[domain.ActiveContract[LfV]]] = {
          import ctx.{jwt, parties, templateIds => templateId}
          for {
            resolved <- toFuture(resolveTemplateId(templateId))
            found <- unsafeRunAsync {
              import dao.logHandler
              import doobie.implicits._, cats.syntax.apply._
              fetch.fetchAndPersist(jwt, parties, List(resolved)) *>
                ContractDao.fetchByKey(parties, resolved, toDbCompatibleJson(contractKey))
            }
          } yield found
        }

        override def search(
            ctx: SearchContext[Set, Id],
            queryParams: Map[String, JsValue],
        ): Source[Error \/ domain.ActiveContract[LfV], NotUsed] = {

          // TODO use `stream` when materializing DBContracts, so we could stream ActiveContracts
          val fv: Future[Vector[domain.ActiveContract[JsValue]]] =
            unsafeRunAsync(searchDb_(fetch, dao.logHandler)(ctx, queryParams))

          Source.future(fv).mapConcat(identity).map(\/.right)
        }

        private[this] def unsafeRunAsync[A](cio: doobie.ConnectionIO[A]) =
          dao.transact(cio).unsafeToFuture()

        private[this] def searchDb_(fetch: ContractsFetch, doobieLog: doobie.LogHandler)(
            ctx: SearchContext[Set, Id],
            queryParams: Map[String, JsValue],
        ): doobie.ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
          import cats.instances.vector._
          import cats.syntax.traverse._
          import doobie.implicits._
          import ctx.{jwt, parties, templateIds}
          for {
            _ <- fetch.fetchAndPersist(jwt, parties, templateIds.toList)
            cts <- templateIds.toVector
              .traverse(tpId => searchDbOneTpId_(doobieLog)(parties, tpId, queryParams))
          } yield cts.flatten
        }

        private[this] def searchDbOneTpId_(doobieLog: doobie.LogHandler)(
            parties: OneAnd[Set, domain.Party],
            templateId: domain.TemplateId.RequiredPkg,
            queryParams: Map[String, JsValue],
        ): doobie.ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
          val predicate = valuePredicate(templateId, queryParams)
          ContractDao.selectContracts(parties, templateId, predicate.toSqlWhereClause)(doobieLog)
        }
      }
  }

  private[this] def searchInMemory(
      jwt: Jwt,
      parties: OneAnd[Set, domain.Party],
      templateIds: Set[domain.TemplateId.RequiredPkg],
      queryParams: InMemoryQuery,
  ): Source[Error \/ domain.ActiveContract[LfValue], NotUsed] = {

    type Ac = domain.ActiveContract[LfValue]
    val empty = (Vector.empty[Error], Vector.empty[Ac])
    import InsertDeleteStep.appendForgettingDeletes

    val funPredicates: Map[domain.TemplateId.RequiredPkg, Ac => Boolean] =
      templateIds.iterator.map(tid => (tid, queryParams.toPredicate(tid))).toMap

    insertDeleteStepSource(jwt, parties, templateIds.toList)
      .map { step =>
        val (errors, converted) = step.toInsertDelete.partitionMapPreservingIds { apiEvent =>
          domain.ActiveContract
            .fromLedgerApi(apiEvent)
            .leftMap(e => Error('searchInMemory, e.shows))
            .flatMap(apiAcToLfAc): Error \/ Ac
        }
        val convertedInserts = converted.inserts filter { ac =>
          funPredicates.get(ac.templateId).exists(_(ac))
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

  private[this] def searchInMemoryOneTpId(
      jwt: Jwt,
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      queryParams: InMemoryQuery.P,
  ): Source[Error \/ domain.ActiveContract[LfValue], NotUsed] =
    searchInMemory(jwt, parties, Set(templateId), InMemoryQuery.Filter(queryParams))

  private[this] sealed abstract class InMemoryQuery extends Product with Serializable {
    import InMemoryQuery._
    def toPredicate(tid: domain.TemplateId.RequiredPkg): P =
      this match {
        case Params(q) =>
          val vp = valuePredicate(tid, q).toFunPredicate
          ac =>
            vp(ac.payload)
        case Filter(p) => p
      }
  }

  private[this] object InMemoryQuery {
    type P = domain.ActiveContract[LfValue] => Boolean
    sealed case class Params(params: Map[String, JsValue]) extends InMemoryQuery
    sealed case class Filter(p: P) extends InMemoryQuery
  }

  private[http] def insertDeleteStepSource(
      jwt: Jwt,
      parties: OneAnd[Set, lar.Party],
      templateIds: List[domain.TemplateId.RequiredPkg],
      startOffset: Option[domain.StartingOffset] = None,
      terminates: Terminates = Terminates.AtLedgerEnd,
  ): Source[ContractStreamStep.LAV1, NotUsed] = {

    val txnFilter = util.Transactions.transactionFilterFor(parties, templateIds)
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

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.fromTryCatchNonFatal(LfValueCodec.apiValueToJsValue(a)).leftMap(e =>
      Error('lfValueToJsValue, e.description))

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

  private type LfValue = lf.value.Value[lf.value.Value.ContractId]

  private final case class SearchValueFormat[-T](encode: T => (Error \/ JsValue))

  final case class SearchContext[Tids[_], Pkgs[_]](
      jwt: Jwt,
      parties: OneAnd[Set, lar.Party],
      templateIds: Tids[domain.TemplateId[Pkgs[String]]],
  )

  // A prototypical abstraction over the in-memory/in-DB split, accounting for
  // the fact that in-memory works with ADT-encoded LF values,
  // whereas in-DB works with JsValues
  private sealed abstract class Search { self =>
    type LfV
    val lfvToJsValue: SearchValueFormat[LfV]

    final def toFinal(implicit ec: ExecutionContext): Search { type LfV = JsValue } = {
      val SearchValueFormat(convert) = lfvToJsValue
      new Search {
        type LfV = JsValue
        override val lfvToJsValue = SearchValueFormat(\/.right)

        override def findByContractId(
            ctx: SearchContext[Option, Option],
            contractId: domain.ContractId,
        ): Future[Option[domain.ActiveContract[LfV]]] =
          self
            .findByContractId(ctx, contractId)
            .flatMap(oac => toFuture(oac traverse (_ traverse convert)))

        override def findByContractKey(
            ctx: SearchContext[Id, Option],
            contractKey: LfValue,
        ): Future[Option[domain.ActiveContract[LfV]]] =
          self
            .findByContractKey(ctx, contractKey)
            .flatMap(oac => toFuture(oac traverse (_ traverse convert)))

        override def search(
            ctx: SearchContext[Set, Id],
            queryParams: Map[String, JsValue],
        ): Source[Error \/ domain.ActiveContract[LfV], NotUsed] =
          self.search(ctx, queryParams) map (_ flatMap (_ traverse convert))
      }
    }

    def findByContractId(
        ctx: SearchContext[Option, Option],
        contractId: domain.ContractId,
    ): Future[Option[domain.ActiveContract[LfV]]]

    def findByContractKey(
        ctx: SearchContext[Id, Option],
        contractKey: LfValue,
    ): Future[Option[domain.ActiveContract[LfV]]]

    def search(
        ctx: SearchContext[Set, Id],
        queryParams: Map[String, JsValue],
    ): Source[Error \/ domain.ActiveContract[LfV], NotUsed]
  }

  case class Error(id: Symbol, message: String)

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"ContractService Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }

  type SearchResult[A] = domain.SyncResponse[Source[A, NotUsed]]
}
