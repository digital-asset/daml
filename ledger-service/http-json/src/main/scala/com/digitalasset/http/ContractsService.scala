// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.http.domain.{ContractTypeId, GetActiveContractsRequest, JwtPayload}
import com.daml.http.json.JsonProtocol.LfValueCodec
import com.daml.http.query.ValuePredicate
import com.daml.fetchcontracts.util.{
  AbsoluteBookmark,
  ContractStreamStep,
  InsertDeleteStep,
  LedgerBegin,
}
import util.{ApiValueToLfValueConverter, toLedgerId}
import com.daml.fetchcontracts.AcsTxStreams.transactionFilter
import com.daml.fetchcontracts.util.ContractStreamStep.{Acs, LiveBegin}
import com.daml.http.util.FutureUtil.toFuture
import com.daml.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.{v1 => api}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.{Metrics, Timed}
import com.daml.scalautil.ExceptionOps._
import com.daml.nonempty.NonEmptyReturningOps._
import scalaz.std.option._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{~>, -\/, OneAnd, OptionT, Show, \/, \/-}
import spray.json.JsValue

import scala.concurrent.{ExecutionContext, Future}
import com.daml.ledger.api.{domain => LedgerApiDomain}
import scalaz.std.scalaFuture._

import com.codahale.metrics.Timer
import doobie.free.{connection => fconn}
import fconn.ConnectionIO

class ContractsService(
    resolveContractTypeId: PackageService.ResolveContractTypeId.AnyKind,
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
      parties: domain.PartySet,
      contractLocator: domain.ContractLocator[LfValue],
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Option[domain.ResolvedContractRef[LfValue]]] = {
    def resolveCt = resolveContractTypeId(jwt, ledgerId)(_)
    def resolveTp = resolveTemplateId(jwt, ledgerId)(_)
    contractLocator match {
      case domain.EnrichedContractKey(templateId, key) =>
        resolveTp(templateId).map(_.toOption.flatten.map(x => -\/(x -> key)))
      case domain.EnrichedContractId(Some(templateId), contractId) =>
        resolveCt(templateId).map(_.toOption.flatten.map(x => \/-(x -> contractId)))
      case domain.EnrichedContractId(None, contractId) =>
        findByContractId(jwt, parties, None, ledgerId, contractId)
          .map(_.map(a => \/-(a.templateId -> a.contractId)))
    }
  }

  def lookup(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      req: domain.FetchRequest[LfValue],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Option[domain.ActiveContract[JsValue]]] = {
    val ledgerId = toLedgerId(jwtPayload.ledgerId)
    val readAs = req.readAs.cata(_.toSet1, jwtPayload.parties)
    req.locator match {
      case domain.EnrichedContractKey(templateId, contractKey) =>
        findByContractKey(jwt, readAs, templateId, ledgerId, contractKey)
      case domain.EnrichedContractId(templateId, contractId) =>
        findByContractId(
          jwt,
          readAs,
          templateId,
          ledgerId,
          contractId,
        )
    }
  }

  private[this] def findByContractKey(
      jwt: Jwt,
      parties: domain.PartySet,
      templateId: ContractTypeId.Template.OptionalPkg,
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
          SearchContext(jwt, parties, templateId, ledgerId),
          contractKey,
        ),
    )
  }

  private[this] def findByContractId(
      jwt: Jwt,
      parties: domain.PartySet,
      templateId: Option[domain.ContractTypeId.OptionalPkg],
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
        ctx: SearchContext.Key,
        contractKey: LfValue,
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: Metrics,
    ): Future[Option[domain.ActiveContract[LfValue]]] = {
      import ctx.{jwt, parties, templateIds => templateId, ledgerId}
      for {
        resolvedTemplateId <- OptionT(
          resolveTemplateId(jwt, ledgerId)(templateId)
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
        ctx: SearchContext.ById,
        contractId: domain.ContractId,
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: Metrics,
    ): Future[Option[domain.ActiveContract[LfValue]]] = {
      import ctx.{jwt, parties, templateIds => templateId, ledgerId}
      for {

        resolvedTemplateIds <- OptionT(
          templateId.cata(
            x =>
              resolveContractTypeId(jwt, ledgerId)(x)
                .map(_.toOption.flatten.map(Set(_))),
            // ignoring interface IDs for all-templates query
            allTemplateIds(lc)(jwt, ledgerId).map(_.toSet[domain.ContractTypeId.Resolved].some),
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

    override def search(ctx: SearchContext.QueryLang, queryParams: Map[String, JsValue])(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: Metrics,
    ) = {
      import ctx.{jwt, parties, templateIds, ledgerId}
      searchInMemory(
        jwt,
        ledgerId,
        parties,
        templateIds,
        InMemoryQuery.Params(queryParams),
      )
    }
  }

  private def lookupResult(
      errorOrAc: Option[Error \/ domain.ActiveContract[LfValue]]
  ): Future[Option[domain.ActiveContract[LfValue]]] =
    errorOrAc traverse (toFuture(_))

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
      parties: domain.PartySet,
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
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[SearchResult[Error \/ domain.ActiveContract[JsValue]]] =
    search(
      jwt,
      toLedgerId(jwtPayload.ledgerId),
      request.readAs.cata((_.toSet1), jwtPayload.parties),
      request.templateIds,
      request.query,
    )

  def search(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: domain.PartySet,
      templateIds: OneAnd[Set, domain.ContractTypeId.OptionalPkg],
      queryParams: Map[String, JsValue],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[SearchResult[Error \/ domain.ActiveContract[JsValue]]] = for {
    res <- resolveContractTypeIds(jwt, ledgerId)(templateIds)
    (resolvedContractTypeIds, unresolvedContractTypeIds) = res

    warnings: Option[domain.UnknownTemplateIds] =
      if (unresolvedContractTypeIds.isEmpty) None
      else Some(domain.UnknownTemplateIds(unresolvedContractTypeIds.toList))
  } yield
    if (resolvedContractTypeIds.isEmpty) {
      domain.ErrorResponse(
        errors = List(ErrorMessages.cannotResolveAnyTemplateId),
        warnings = warnings,
        status = StatusCodes.BadRequest,
      )
    } else {
      val searchCtx = SearchContext(jwt, parties, resolvedContractTypeIds, ledgerId)
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
            ctx: SearchContext.ById,
            contractId: domain.ContractId,
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID],
            metrics: Metrics,
        ): Future[Option[domain.ActiveContract[LfV]]] = {
          import ctx.{jwt, parties, templateIds => otemplateId, ledgerId}
          val dbQueried = for {
            templateId <- OptionT(Future.successful(otemplateId))
            resolved <- OptionT(
              resolveContractTypeId(jwt, ledgerId)(templateId).map(_.toOption.flatten)
            )
            res <- OptionT(unsafeRunAsync {
              import doobie.implicits._, cats.syntax.apply._
              // a single contractId is either present or not; we would only need
              // to fetchAndPersistBracket if we were looking up multiple cids
              // in the same HTTP request, and they would all have to be bracketed once -SC
              timed(
                metrics.daml.HttpJsonApi.Db.fetchByIdFetch,
                fetch.fetchAndPersist(jwt, ledgerId, parties, List(resolved)),
              ) *>
                timed(
                  metrics.daml.HttpJsonApi.Db.fetchByIdQuery,
                  ContractDao.fetchById(parties, resolved, contractId),
                )
            })
          } yield res
          dbQueried.orElse {
            // we need a template ID to update the database
            OptionT(SearchInMemory.toFinal.findByContractId(ctx, contractId))
          }.run
        }

        override def findByContractKey(
            ctx: SearchContext.Key,
            contractKey: LfValue,
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID],
            metrics: Metrics,
        ): Future[Option[domain.ActiveContract[LfV]]] = {
          import ctx.{jwt, parties, templateIds => templateId, ledgerId}, com.daml.lf.crypto.Hash
          for {
            resolved <- resolveTemplateId(jwt, ledgerId)(templateId).map(_.toOption.flatten.get)
            found <- unsafeRunAsync {
              import doobie.implicits._, cats.syntax.apply._
              // it is possible for the contract under a given key to have been
              // replaced concurrently with a contract unobservable by parties, i.e.
              // the DB contains two contracts with the same key.  However, this doesn't
              // ever yield an _inconsistent_ result, merely one that is slightly back-in-time,
              // which is true of all json-api responses.  Again, if we were checking for
              // multiple template/contract-key pairs in a single request, they would all
              // have to be contained within a single fetchAndPersistBracket -SC
              timed(
                metrics.daml.HttpJsonApi.Db.fetchByKeyFetch,
                fetch.fetchAndPersist(jwt, ledgerId, parties, List(resolved)),
              ) *>
                timed(
                  metrics.daml.HttpJsonApi.Db.fetchByKeyQuery,
                  ContractDao.fetchByKey(
                    parties,
                    resolved,
                    Hash.assertHashContractKey(toLedgerApiValue(resolved), contractKey),
                  ),
                )
            }
          } yield found
        }

        override def search(
            ctx: SearchContext.QueryLang,
            queryParams: Map[String, JsValue],
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID],
            metrics: Metrics,
        ): Source[Error \/ domain.ActiveContract[LfV], NotUsed] = {

          // TODO use `stream` when materializing DBContracts, so we could stream ActiveContracts
          val fv: Future[Vector[domain.ActiveContract[JsValue]]] =
            unsafeRunAsync(searchDb_(fetch)(ctx, queryParams))

          Source.future(fv).mapConcat(identity).map(\/.right)
        }

        private[this] def unsafeRunAsync[A](cio: doobie.ConnectionIO[A]) =
          dao.transact(cio).unsafeToFuture()

        private[this] def timed[A](
            timer: Timer,
            it: doobie.ConnectionIO[A],
        ): doobie.ConnectionIO[A] = {
          for {
            _ <- fconn.pure(())
            ctx <- fconn.pure(timer.time())
            res <- it
            _ <- fconn.pure(ctx.stop())
          } yield res
        }

        private[this] def searchDb_(fetch: ContractsFetch)(
            ctx: SearchContext.QueryLang,
            queryParams: Map[String, JsValue],
        )(implicit
            lc: LoggingContextOf[InstanceUUID],
            metrics: Metrics,
        ): doobie.ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
          import cats.instances.vector._
          import cats.syntax.traverse._
          import doobie.implicits._
          import ctx.{jwt, parties, templateIds, ledgerId}
          for {
            cts <- fetch.fetchAndPersistBracket(
              jwt,
              ledgerId,
              parties,
              templateIds.toList,
              Lambda[ConnectionIO ~> ConnectionIO](
                timed(metrics.daml.HttpJsonApi.Db.searchFetch, _)
              ),
            ) {
              case LedgerBegin =>
                fconn.pure(Vector.empty[Vector[domain.ActiveContract[JsValue]]])
              case AbsoluteBookmark(_) =>
                timed(
                  metrics.daml.HttpJsonApi.Db.searchQuery,
                  templateIds.toVector
                    .traverse(tpId => searchDbOneTpId_(parties, tpId, queryParams)),
                )
            }
          } yield cts.flatten
        }

        private[this] def searchDbOneTpId_(
            parties: domain.PartySet,
            templateId: domain.ContractTypeId.Resolved,
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
      parties: domain.PartySet,
      templateIds: Set[domain.TemplateId.Resolved],
      queryParams: InMemoryQuery,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[InternalError \/ domain.ActiveContract[LfValue], NotUsed] = {

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
            .leftMap(e => InternalError(Symbol("searchInMemory"), e.shows))
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
      parties: domain.PartySet,
      templateId: domain.TemplateId.Resolved,
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
      parties: domain.PartySet,
      templateIds: List[domain.ContractTypeId.Resolved],
  )(implicit lc: LoggingContextOf[InstanceUUID]): Source[ContractStreamStep.LAV1, NotUsed] = {
    val txnFilter = transactionFilter(parties, templateIds)
    getActiveContracts(jwt, ledgerId, txnFilter, true)(lc)
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
      parties: domain.PartySet,
      templateIds: List[domain.ContractTypeId.Resolved],
      startOffset: Option[domain.StartingOffset] = None,
      terminates: Terminates = Terminates.AtLedgerEnd,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[ContractStreamStep.LAV1, NotUsed] = {

    val txnFilter = transactionFilter(parties, templateIds)
    def source = getActiveContracts(jwt, ledgerId, txnFilter, true)(lc)

    val transactionsSince
        : api.ledger_offset.LedgerOffset => Source[api.transaction.Transaction, NotUsed] =
      getCreatesAndArchivesSince(
        jwt,
        ledgerId,
        txnFilter,
        _: api.ledger_offset.LedgerOffset,
        terminates,
      )(lc)

    import com.daml.fetchcontracts.AcsTxStreams.{
      acsFollowingAndBoundary,
      transactionsFollowingBoundary,
    }, com.daml.fetchcontracts.util.GraphExtensions._
    val contractsAndBoundary = startOffset.cata(
      so =>
        Source
          .single(AbsoluteBookmark(so.offset))
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
      .leftMap(e => InternalError(Symbol("apiAcToLfAc"), e.shows))

  private[http] def valuePredicate(
      templateId: domain.TemplateId.RequiredPkg,
      q: Map[String, JsValue],
  ): query.ValuePredicate =
    ValuePredicate.fromTemplateJsObject(q, templateId, lookupType)

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.attempt(LfValueCodec.apiValueToJsValue(a))(e =>
      InternalError(Symbol("lfValueToJsValue"), e.description)
    )

  private[http] def resolveContractTypeIds[Tid <: domain.ContractTypeId.OptionalPkg](
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(
      xs: OneAnd[Set, Tid]
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[(Set[domain.ContractTypeId.Resolved], Set[Tid])] = {
    import scalaz.syntax.traverse._
    import scalaz.std.iterable._
    import scalaz.std.list._, scalaz.std.scalaFuture._

    xs.toList
      .traverse { x =>
        resolveContractTypeId(jwt, ledgerId)(x)
          .map(_.toOption.flatten.toLeft(x)): Future[
          Either[domain.ContractTypeId.Resolved, Tid]
        ]
      }
      .map(_.toSet[Either[domain.ContractTypeId.Resolved, Tid]].partitionMap(a => a))
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
      ledgerId: LedgerApiDomain.LedgerId,
  )

  private object SearchContext {
    type QueryLang = SearchContext[
      Set[domain.ContractTypeId.Resolved]
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

    final def toFinal(implicit
        ec: ExecutionContext
    ): Search { type LfV = JsValue } = {
      val SearchValueFormat(convert) = lfvToJsValue
      new Search {
        type LfV = JsValue
        override val lfvToJsValue = SearchValueFormat(\/.right)

        override def findByContractId(
            ctx: SearchContext.ById,
            contractId: domain.ContractId,
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID],
            metrics: Metrics,
        ): Future[Option[domain.ActiveContract[LfV]]] =
          self
            .findByContractId(ctx, contractId)
            .flatMap(oac => toFuture(oac traverse (_ traverse convert)))

        override def findByContractKey(
            ctx: SearchContext.Key,
            contractKey: LfValue,
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID],
            metrics: Metrics,
        ): Future[Option[domain.ActiveContract[LfV]]] =
          self
            .findByContractKey(ctx, contractKey)
            .flatMap(oac => toFuture(oac traverse (_ traverse convert)))

        override def search(
            ctx: SearchContext.QueryLang,
            queryParams: Map[String, JsValue],
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID],
            metrics: Metrics,
        ): Source[Error \/ domain.ActiveContract[LfV], NotUsed] =
          self.search(ctx, queryParams) map (_ flatMap (_ traverse convert))
      }
    }

    def findByContractId(
        ctx: SearchContext.ById,
        contractId: domain.ContractId,
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: Metrics,
    ): Future[Option[domain.ActiveContract[LfV]]]

    def findByContractKey(
        ctx: SearchContext.Key,
        contractKey: LfValue,
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: Metrics,
    ): Future[Option[domain.ActiveContract[LfV]]]

    def search(
        ctx: SearchContext.QueryLang,
        queryParams: Map[String, JsValue],
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: Metrics,
    ): Source[Error \/ domain.ActiveContract[LfV], NotUsed]
  }

  final case class Error(id: Symbol, message: String)
  private type InternalError = Error
  private[http] val InternalError: Error.type = Error

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"ContractService Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }

  type SearchResult[A] = domain.SyncResponse[Source[A, NotUsed]]
}
