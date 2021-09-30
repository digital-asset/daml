// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl._
import akka.stream.Materializer
import com.daml.lf
import com.daml.http.LedgerClientJwt.Terminates
import com.daml.http.dbbackend.ContractDao
import com.daml.http.domain.TemplateId.toLedgerApiValue
import com.daml.http.domain.{GetActiveContractsRequest, JwtPayload, TemplateId}
import com.daml.http.json.JsonProtocol.LfValueCodec
import com.daml.http.query.ValuePredicate
import util.{
  AbsoluteBookmark,
  ApiValueToLfValueConverter,
  ContractStreamStep,
  InsertDeleteStep,
  toLedgerId,
}
import com.daml.fetchcontracts.util.ContractStreamStep.{Acs, LiveBegin}
import com.daml.http.util.FutureUtil.toFuture
import com.daml.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.{v1 => api}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.{Metrics, Timed}
import com.daml.scalautil.ExceptionOps._
import scalaz.Id.Id
import scalaz.std.option._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, OneAnd, OptionT, Show, Tag, \/, \/-}
import spray.json.JsValue

import scala.collection.compat._
import scala.concurrent.{ExecutionContext, Future}
import com.daml.ledger.api.{domain => LedgerApiDomain}
import scalaz.std.scalaFuture._

class ContractsService(
    resolveTemplateId: PackageService.ResolveTemplateId,
    allTemplateIds: PackageService.AllTemplateIds,
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    getTermination: LedgerClientJwt.GetTermination,
    lookupType: query.ValuePredicate.TypeLookup,
    contractDao: Option[dbbackend.ContractDao],
)(implicit ec: ExecutionContext, mat: Materializer) {
  private[this] val logger = ContextualizedLogger.get(getClass)

  import ContractsService._

  type CompiledPredicates = Map[domain.TemplateId.RequiredPkg, query.ValuePredicate]

  private[http] val daoAndFetch: Option[(dbbackend.ContractDao, ContractsFetch)] = contractDao.map {
    dao =>
      import dao.{logHandler, jdbcDriver}
      (
        dao,
        new ContractsFetch(
          getActiveContracts,
          getCreatesAndArchivesSince,
          getTermination,
        ),
      )
  }

  def resolveContractReference(
      jwt: Jwt,
      parties: OneAnd[Set, domain.Party],
      contractLocator: domain.ContractLocator[LfValue],
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Option[domain.ResolvedContractRef[LfValue]]] = {
    val resolve = resolveTemplateId(lc)(jwt, ledgerId)
    contractLocator match {
      case domain.EnrichedContractKey(templateId, key) =>
        resolve(templateId).map(_.toOption.flatten.map(x => -\/(x -> key)))
      case domain.EnrichedContractId(Some(templateId), contractId) =>
        resolve(templateId).map(_.toOption.flatten.map(x => \/-(x -> contractId)))
      case domain.EnrichedContractId(None, contractId) =>
        findByContractId(jwt, parties, None, ledgerId, contractId)
          .map(_.map(a => \/-(a.templateId -> a.contractId)))
    }
  }

  def lookup(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      contractLocator: domain.ContractLocator[LfValue],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Option[domain.ActiveContract[JsValue]]] = {
    val ledgerId = toLedgerId(jwtPayload.ledgerId)
    contractLocator match {
      case domain.EnrichedContractKey(templateId, contractKey) =>
        findByContractKey(jwt, jwtPayload.parties, templateId, ledgerId, contractKey)
      case domain.EnrichedContractId(templateId, contractId) =>
        findByContractId(
          jwt,
          jwtPayload.parties,
          templateId,
          ledgerId,
          contractId,
        )
    }
  }

  private[this] def findByContractKey(
      jwt: Jwt,
      parties: OneAnd[Set, lar.Party],
      templateId: TemplateId.OptionalPkg,
      ledgerId: LedgerApiDomain.LedgerId,
      contractKey: LfValue,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Option[domain.ActiveContract[JsValue]]] = {
    Timed.future(
      metrics.daml.HttpJsonApi.dbFindByContractKey,
      search.toFinal
        .findByContractKey(
          SearchContext[Id, Option](jwt, parties, templateId, ledgerId),
          contractKey,
        ),
    )
  }

  private[this] def findByContractId(
      jwt: Jwt,
      parties: OneAnd[Set, lar.Party],
      templateId: Option[domain.TemplateId.OptionalPkg],
      ledgerId: LedgerApiDomain.LedgerId,
      contractId: domain.ContractId,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Option[domain.ActiveContract[JsValue]]] = {
    Timed.future(
      metrics.daml.HttpJsonApi.dbFindByContractId,
      search.toFinal.findByContractId(SearchContext(jwt, parties, templateId, ledgerId), contractId),
    )
  }

  private[this] def search: Search = SearchDb getOrElse SearchInMemory

  private object SearchInMemory extends Search {
    type LfV = LfValue
    override val lfvToJsValue = SearchValueFormat(lfValueToJsValue)

    override def findByContractKey(
        ctx: SearchContext[Id, Option],
        contractKey: LfValue,
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID]
    ): Future[Option[domain.ActiveContract[LfValue]]] = {
      import ctx.{jwt, parties, templateIds => templateId, ledgerId}
      for {
        resolvedTemplateId <- OptionT(
          resolveTemplateId(lc)(jwt, ledgerId)(templateId)
            .map(
              _.toOption.flatten
            )
        )

        predicate = domain.ActiveContract.matchesKey(contractKey) _

        result <- OptionT(
          searchInMemoryOneTpId(jwt, ledgerId, parties, resolvedTemplateId, predicate)
            .runWith(Sink.headOption)
            .flatMap(lookupResult)
        )

      } yield result
    }.run

    override def findByContractId(
        ctx: SearchContext[Option, Option],
        contractId: domain.ContractId,
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID]
    ): Future[Option[domain.ActiveContract[LfValue]]] = {
      import ctx.{jwt, parties, templateIds => templateId, ledgerId}
      for {

        resolvedTemplateIds <- OptionT(
          templateId.cata(
            x =>
              resolveTemplateId(lc)(jwt, ledgerId)(x)
                .map(_.toOption.flatten.map(Set(_))),
            allTemplateIds(lc)(jwt, ledgerId).map(_.some),
          )
        )

        result <- OptionT(
          searchInMemory(
            jwt,
            ledgerId,
            parties,
            resolvedTemplateIds,
            InMemoryQuery.Filter(isContractId(contractId)),
          )
            .runWith(Sink.headOption)
            .flatMap(lookupResult)
        )

      } yield result
    }.run

    override def search(ctx: SearchContext[Set, Id], queryParams: Map[String, JsValue])(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID]
    ) = {
      import ctx.{jwt, parties, templateIds, ledgerId}
      searchInMemory(jwt, ledgerId, parties, templateIds, InMemoryQuery.Params(queryParams))
    }
  }

  private def lookupResult(
      errorOrAc: Option[Error \/ domain.ActiveContract[LfValue]]
  ): Future[Option[domain.ActiveContract[LfValue]]] = {
    errorOrAc.cata(x => toFuture(x).map(Some(_)), Future.successful(None))
  }

  private def isContractId(k: domain.ContractId)(a: domain.ActiveContract[LfValue]): Boolean =
    (a.contractId: domain.ContractId) == k

  def retrieveAll(
      jwt: Jwt,
      jwtPayload: JwtPayload,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): SearchResult[Error \/ domain.ActiveContract[LfValue]] =
    retrieveAll(jwt, toLedgerId(jwtPayload.ledgerId), jwtPayload.parties)

  def retrieveAll(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: OneAnd[Set, domain.Party],
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): SearchResult[Error \/ domain.ActiveContract[LfValue]] =
    domain.OkResponse(
      Source
        .future(allTemplateIds(lc)(jwt, ledgerId))
        .flatMapConcat(x =>
          Source(x)
            .flatMapConcat(x => searchInMemoryOneTpId(jwt, ledgerId, parties, x, _ => true))
        )
    )

  def search(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      request: GetActiveContractsRequest,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[SearchResult[Error \/ domain.ActiveContract[JsValue]]] =
    search(
      jwt,
      toLedgerId(jwtPayload.ledgerId),
      jwtPayload.parties,
      request.templateIds,
      request.query,
    )

  def search(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: OneAnd[Set, domain.Party],
      templateIds: OneAnd[Set, domain.TemplateId.OptionalPkg],
      queryParams: Map[String, JsValue],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[SearchResult[Error \/ domain.ActiveContract[JsValue]]] = for {
    res <- resolveTemplateIds(jwt, ledgerId)(templateIds)
    (resolvedTemplateIds, unresolvedTemplateIds) = res

    warnings: Option[domain.UnknownTemplateIds] =
      if (unresolvedTemplateIds.isEmpty) None
      else Some(domain.UnknownTemplateIds(unresolvedTemplateIds.toList))
  } yield
    if (resolvedTemplateIds.isEmpty) {
      domain.ErrorResponse(
        errors = List(ErrorMessages.cannotResolveAnyTemplateId),
        warnings = warnings,
        status = StatusCodes.BadRequest,
      )
    } else {
      val searchCtx = SearchContext[Set, Id](jwt, parties, resolvedTemplateIds, ledgerId)
      val source = search.toFinal.search(searchCtx, queryParams)
      domain.OkResponse(source, warnings)
    }

  private[this] val SearchDb: Option[Search { type LfV = JsValue }] = daoAndFetch map {
    case (dao, fetch) =>
      new Search {
        import dao.{logHandler => doobieLog, jdbcDriver}
        // we store create arguments as JSON in DB, that is why it is `JsValue` in the result
        type LfV = JsValue
        override val lfvToJsValue = SearchValueFormat(\/.right)

        override def findByContractId(
            ctx: SearchContext[Option, Option],
            contractId: domain.ContractId,
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID]
        ): Future[Option[domain.ActiveContract[LfV]]] = {
          import ctx.{jwt, parties, templateIds => otemplateId, ledgerId}
          import scalaz.Scalaz._
          val dbQueried = for {
            templateId <- OptionT(Future.successful(otemplateId))
            resolved <- OptionT(
              resolveTemplateId(lc)(jwt, ledgerId)(templateId).map(_.toOption.flatten)
            )
            res <- OptionT(unsafeRunAsync {
              import doobie.implicits._, cats.syntax.apply._
              fetch.fetchAndPersist(jwt, ledgerId, parties, List(resolved)) *>
                ContractDao.fetchById(parties, resolved, contractId)
            })
          } yield res
          dbQueried.orElse {
            // we need a template ID to update the database
            OptionT(SearchInMemory.toFinal.findByContractId(ctx, contractId))
          }.run
        }

        override def findByContractKey(
            ctx: SearchContext[Id, Option],
            contractKey: LfValue,
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID]
        ): Future[Option[domain.ActiveContract[LfV]]] = {
          import ctx.{jwt, parties, templateIds => templateId, ledgerId}, com.daml.lf.crypto.Hash
          for {
            resolved <- resolveTemplateId(lc)(jwt, ledgerId)(templateId).map(_.toOption.flatten.get)
            found <- unsafeRunAsync {
              import doobie.implicits._, cats.syntax.apply._
              fetch.fetchAndPersist(jwt, ledgerId, parties, List(resolved)) *>
                ContractDao.fetchByKey(
                  parties,
                  resolved,
                  Hash.assertHashContractKey(toLedgerApiValue(resolved), contractKey),
                )
            }
          } yield found
        }

        override def search(
            ctx: SearchContext[Set, Id],
            queryParams: Map[String, JsValue],
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID]
        ): Source[Error \/ domain.ActiveContract[LfV], NotUsed] = {

          // TODO use `stream` when materializing DBContracts, so we could stream ActiveContracts
          val fv: Future[Vector[domain.ActiveContract[JsValue]]] =
            unsafeRunAsync(searchDb_(fetch)(ctx, queryParams))

          Source.future(fv).mapConcat(identity).map(\/.right)
        }

        private[this] def unsafeRunAsync[A](cio: doobie.ConnectionIO[A]) =
          dao.transact(cio).unsafeToFuture()

        private[this] def searchDb_(fetch: ContractsFetch)(
            ctx: SearchContext[Set, Id],
            queryParams: Map[String, JsValue],
        )(implicit
            lc: LoggingContextOf[InstanceUUID]
        ): doobie.ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
          import cats.instances.vector._
          import cats.syntax.traverse._
          import doobie.implicits._
          import ctx.{jwt, parties, templateIds, ledgerId}
          for {
            _ <- fetch.fetchAndPersist(jwt, ledgerId, parties, templateIds.toList)
            cts <- templateIds.toVector
              .traverse(tpId => searchDbOneTpId_(parties, tpId, queryParams))
          } yield cts.flatten
        }

        private[this] def searchDbOneTpId_(
            parties: OneAnd[Set, domain.Party],
            templateId: domain.TemplateId.RequiredPkg,
            queryParams: Map[String, JsValue],
        )(implicit
            lc: LoggingContextOf[InstanceUUID]
        ): doobie.ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
          val predicate = valuePredicate(templateId, queryParams)
          ContractDao.selectContracts(parties, templateId, predicate.toSqlWhereClause)
        }
      }
  }

  private[this] def searchInMemory(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: OneAnd[Set, domain.Party],
      templateIds: Set[domain.TemplateId.RequiredPkg],
      queryParams: InMemoryQuery,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[Error \/ domain.ActiveContract[LfValue], NotUsed] = {

    logger.debug(
      s"Searching in memory, parties: $parties, templateIds: $templateIds, queryParms: $queryParams"
    )

    type Ac = domain.ActiveContract[LfValue]
    val empty = (Vector.empty[Error], Vector.empty[Ac])
    import InsertDeleteStep.appendForgettingDeletes

    val funPredicates: Map[domain.TemplateId.RequiredPkg, Ac => Boolean] =
      templateIds.iterator.map(tid => (tid, queryParams.toPredicate(tid))).toMap

    insertDeleteStepSource(jwt, ledgerId, parties, templateIds.toList)
      .map { step =>
        val (errors, converted) = step.toInsertDelete.partitionMapPreservingIds { apiEvent =>
          domain.ActiveContract
            .fromLedgerApi(apiEvent)
            .leftMap(e => Error(Symbol("searchInMemory"), e.shows))
            .flatMap(apiAcToLfAc): Error \/ Ac
        }
        val convertedInserts = converted.inserts filter { ac =>
          funPredicates.get(ac.templateId).exists(_(ac))
        }
        (errors, converted.copy(inserts = convertedInserts))
      }
      .fold(empty) { case ((errL, stepL), (errR, stepR)) =>
        (errL ++ errR, appendForgettingDeletes(stepL, stepR))
      }
      .mapConcat { case (err, inserts) =>
        inserts.map(\/-(_)) ++ err.map(-\/(_))
      }
  }

  private[this] def searchInMemoryOneTpId(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      queryParams: InMemoryQuery.P,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[Error \/ domain.ActiveContract[LfValue], NotUsed] =
    searchInMemory(jwt, ledgerId, parties, Set(templateId), InMemoryQuery.Filter(queryParams))

  private[this] sealed abstract class InMemoryQuery extends Product with Serializable {
    import InMemoryQuery._
    def toPredicate(tid: domain.TemplateId.RequiredPkg): P =
      this match {
        case Params(q) =>
          val vp = valuePredicate(tid, q).toFunPredicate
          ac => vp(ac.payload)
        case Filter(p) => p
      }
  }

  private[this] object InMemoryQuery {
    type P = domain.ActiveContract[LfValue] => Boolean
    sealed case class Params(params: Map[String, JsValue]) extends InMemoryQuery
    sealed case class Filter(p: P) extends InMemoryQuery
  }

  private[http] def liveAcsAsInsertDeleteStepSource(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: OneAnd[Set, lar.Party],
      templateIds: List[domain.TemplateId.RequiredPkg],
  ): Source[ContractStreamStep.LAV1, NotUsed] = {
    val txnFilter = util.Transactions.transactionFilterFor(parties, templateIds)
    getActiveContracts(jwt, ledgerId, txnFilter, true)
      .map { case GetActiveContractsResponse(offset, _, activeContracts) =>
        if (activeContracts.nonEmpty) Acs(activeContracts.toVector)
        else LiveBegin(AbsoluteBookmark(domain.Offset(offset)))
      }
  }

  /** An ACS ++ transaction stream of `templateIds`, starting at `startOffset`
    * and ending at `terminates`.
    */
  private[http] def insertDeleteStepSource(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: OneAnd[Set, lar.Party],
      templateIds: List[domain.TemplateId.RequiredPkg],
      startOffset: Option[domain.StartingOffset] = None,
      terminates: Terminates = Terminates.AtLedgerEnd,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[ContractStreamStep.LAV1, NotUsed] = {

    val txnFilter = util.Transactions.transactionFilterFor(parties, templateIds)
    def source = getActiveContracts(jwt, ledgerId, txnFilter, true)

    val transactionsSince
        : api.ledger_offset.LedgerOffset => Source[api.transaction.Transaction, NotUsed] =
      getCreatesAndArchivesSince(
        jwt,
        ledgerId,
        txnFilter,
        _: api.ledger_offset.LedgerOffset,
        terminates,
      )

    import ContractsFetch.{acsFollowingAndBoundary, transactionsFollowingBoundary},
    com.daml.fetchcontracts.util.GraphExtensions._
    val contractsAndBoundary = startOffset.cata(
      so =>
        Source
          .single(Tag unsubst util.AbsoluteBookmark(so.offset))
          .viaMat(transactionsFollowingBoundary(transactionsSince).divertToHead)(Keep.right),
      source.viaMat(acsFollowingAndBoundary(transactionsSince).divertToHead)(Keep.right),
    )
    contractsAndBoundary mapMaterializedValue { fob =>
      fob.foreach(a => logger.debug(s"contracts fetch completed at: ${a.toString}"))
      NotUsed
    }
  }

  private def apiAcToLfAc(
      ac: domain.ActiveContract[ApiValue]
  ): Error \/ domain.ActiveContract[LfValue] =
    ac.traverse(ApiValueToLfValueConverter.apiValueToLfValue)
      .leftMap(e => Error(Symbol("apiAcToLfAc"), e.shows))

  private[http] def valuePredicate(
      templateId: domain.TemplateId.RequiredPkg,
      q: Map[String, JsValue],
  ): query.ValuePredicate =
    ValuePredicate.fromTemplateJsObject(q, templateId, lookupType)

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.attempt(LfValueCodec.apiValueToJsValue(a))(e =>
      Error(Symbol("lfValueToJsValue"), e.description)
    )

  private[http] def resolveTemplateIds[Tid <: domain.TemplateId.OptionalPkg](
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(
      xs: OneAnd[Set, Tid]
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[(Set[domain.TemplateId.RequiredPkg], Set[Tid])] = {
    import scalaz.syntax.traverse._
    import scalaz.std.iterable._
    import scalaz.std.list._, scalaz.std.scalaFuture._

    xs.toList
      .traverse { x =>
        resolveTemplateId(lc)(jwt, ledgerId)(x)
          .map(_.toOption.flatten.toLeft(x)): Future[Either[domain.TemplateId.RequiredPkg, Tid]]
      }
      .map(_.toSet[Either[domain.TemplateId.RequiredPkg, Tid]].partitionMap(a => a))
  }
}

object ContractsService {
  private type ApiValue = api.value.Value

  private type LfValue = lf.value.Value

  private final case class SearchValueFormat[-T](encode: T => (Error \/ JsValue))

  final case class SearchContext[Tids[_], Pkgs[_]](
      jwt: Jwt,
      parties: OneAnd[Set, lar.Party],
      templateIds: Tids[domain.TemplateId[Pkgs[String]]],
      ledgerId: LedgerApiDomain.LedgerId,
  )

  // A prototypical abstraction over the in-memory/in-DB split, accounting for
  // the fact that in-memory works with ADT-encoded LF values,
  // whereas in-DB works with JsValues
  private sealed abstract class Search { self =>
    type LfV
    val lfvToJsValue: SearchValueFormat[LfV]

    final def toFinal(implicit
        ec: ExecutionContext
    ): Search { type LfV = JsValue } = {
      val SearchValueFormat(convert) = lfvToJsValue
      new Search {
        type LfV = JsValue
        override val lfvToJsValue = SearchValueFormat(\/.right)

        override def findByContractId(
            ctx: SearchContext[Option, Option],
            contractId: domain.ContractId,
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID]
        ): Future[Option[domain.ActiveContract[LfV]]] =
          self
            .findByContractId(ctx, contractId)
            .flatMap(oac => toFuture(oac traverse (_ traverse convert)))

        override def findByContractKey(
            ctx: SearchContext[Id, Option],
            contractKey: LfValue,
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID]
        ): Future[Option[domain.ActiveContract[LfV]]] =
          self
            .findByContractKey(ctx, contractKey)
            .flatMap(oac => toFuture(oac traverse (_ traverse convert)))

        override def search(
            ctx: SearchContext[Set, Id],
            queryParams: Map[String, JsValue],
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID]
        ): Source[Error \/ domain.ActiveContract[LfV], NotUsed] =
          self.search(ctx, queryParams) map (_ flatMap (_ traverse convert))
      }
    }

    def findByContractId(
        ctx: SearchContext[Option, Option],
        contractId: domain.ContractId,
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID]
    ): Future[Option[domain.ActiveContract[LfV]]]

    def findByContractKey(
        ctx: SearchContext[Id, Option],
        contractKey: LfValue,
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID]
    ): Future[Option[domain.ActiveContract[LfV]]]

    def search(
        ctx: SearchContext[Set, Id],
        queryParams: Map[String, JsValue],
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID]
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
