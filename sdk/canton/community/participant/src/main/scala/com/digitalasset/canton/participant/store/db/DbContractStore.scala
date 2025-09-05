// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{EitherT, OptionT}
import cats.implicits.toTraverseOps
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.config.CantonRequireTypes.String2066
import com.digitalasset.canton.config.{
  BatchAggregatorConfig,
  BatchingConfig,
  CacheConfig,
  ProcessingTimeout,
}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.resource.DbStorage.{DbAction, SQLActionBuilderChain}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.{DbBulkUpdateProcessor, DbDeserializationException}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.EitherUtil.RichEitherIterable
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{BatchAggregator, ErrorUtil, MonadUtil, TryUtil}
import com.digitalasset.canton.{LfPartyId, checked}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Try}

class DbContractStore(
    override protected val storage: DbStorage,
    cacheConfig: CacheConfig,
    dbQueryBatcherConfig: BatchAggregatorConfig,
    insertBatchAggregatorConfig: BatchAggregatorConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(protected implicit val ec: ExecutionContext)
    extends ContractStore
    with DbStore { self =>

  import DbStorage.Implicits.*
  import storage.api.*
  import storage.converters.*

  private val profile = storage.profile

  override protected[store] def logger: TracedLogger = super.logger

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def contractGetResult(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[ContractInstance] = GetResult { r =>
    ContractInstance.decodeWithCreatedAt(ByteString.copyFrom(r.<<[Array[Byte]])) match {
      case Right(contract) => contract
      case Left(e) => throw new DbDeserializationException(s"Invalid contract instance: $e")
    }
  }

  implicit def contractSetParameter: SetParameter[ContractInstance] = (c, pp) => pp >> c.encoded

  private val cache: ScaffeineCache.TunnelledAsyncCache[LfContractId, Option[ContractInstance]] =
    ScaffeineCache.buildMappedAsync[LfContractId, Option[ContractInstance]](
      cacheConfig.buildScaffeine()
    )(logger, "DbContractStore.cache")

  private def invalidateCache(key: LfContractId): Unit =
    cache.invalidate(key)

  // batch aggregator used for single point queries: damle will run many "lookups"
  // during interpretation. they will hit the db like a nail gun. the batch
  // aggregator will limit the number of parallel queries to the db and "batch them"
  // together. so if there is high load with a lot of interpretation happening in parallel
  // batching will kick in.
  private val batchAggregatorLookup = {
    val processor: BatchAggregator.Processor[LfContractId, Option[ContractInstance]] =
      new BatchAggregator.Processor[LfContractId, Option[ContractInstance]] {
        override val kind: String = "serializable contract"
        override def logger: TracedLogger = DbContractStore.this.logger

        override def executeBatch(ids: NonEmpty[Seq[Traced[LfContractId]]])(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): FutureUnlessShutdown[Iterable[Option[ContractInstance]]] =
          lookupManyUncachedInternal(ids.map(_.value))

        override def prettyItem: Pretty[LfContractId] = implicitly
      }
    BatchAggregator(
      processor,
      dbQueryBatcherConfig,
    )
  }

  private val contractsBaseQuery =
    sql"""select instance from par_contracts"""

  private def lookupQuery(
      ids: NonEmpty[Seq[LfContractId]]
  ): DbAction.ReadOnly[Seq[Option[ContractInstance]]] = {
    import DbStorage.Implicits.BuilderChain.*

    val inClause = DbStorage.toInClause("contract_id", ids)
    (contractsBaseQuery ++ sql" where " ++ inClause)
      .as[ContractInstance]
      .map { contracts =>
        val foundContracts = contracts
          .map(contract => (contract.contractId, contract))
          .toMap
        ids.map(foundContracts.get)
      }
  }

  private def bulkLookupQuery(
      ids: NonEmpty[Seq[LfContractId]]
  ): DbAction.ReadOnly[immutable.Iterable[ContractInstance]] = {
    val inClause = DbStorage.toInClause("contract_id", ids)
    import DbStorage.Implicits.BuilderChain.*
    val query =
      contractsBaseQuery ++ sql" where " ++ inClause
    query.as[ContractInstance]
  }

  def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, ContractInstance] =
    OptionT(cache.getFuture(id, _ => batchAggregatorLookup.run(id)))

  override def lookupManyExistingUncached(
      ids: Seq[LfContractId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LfContractId, List[ContractInstance]] =
    NonEmpty
      .from(ids)
      .map(ids =>
        EitherT(lookupManyUncachedInternal(ids).map(ids.toList.zip(_).traverse {
          case (id, contract) =>
            contract.toRight(id)
        }))
      )
      .getOrElse(EitherT.rightT(List.empty))

  private def lookupManyUncachedInternal(
      ids: NonEmpty[Seq[LfContractId]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[Option[ContractInstance]]] =
    MonadUtil
      .batchedSequentialTraverseNE(
        parallelism = BatchingConfig().parallelism,
        // chunk the ids to query to avoid hitting prepared statement limits
        chunkSize = DbStorage.maxSqlParameters,
      )(
        ids
      )(chunk => storage.query(lookupQuery(chunk), functionFullName))

  override def find(
      exactId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[List[ContractInstance]] = {

    import DbStorage.Implicits.BuilderChain.*

    // If filter is set: returns a conjunctive (`and` prepended) constraint on attribute `name`.
    // Otherwise empty sql action.
    def createConjunctiveFilter(
        name: String,
        filter: Option[String],
    ): Option[SQLActionBuilderChain] =
      filter
        .map { f =>
          sql" #$name " ++ (f match {
            case rs if rs.startsWith("!") => sql"= ${rs.drop(1)}" // Must be equal
            case rs if rs.startsWith("^") => sql"""like ${rs.drop(1) + "%"}""" // Starts with
            case rs => sql"""like ${"%" + rs + "%"}""" // Contains
          })
        }

    val pkgFilter = createConjunctiveFilter("package_id", filterPackage)
    val templateFilter = createConjunctiveFilter("template_id", filterTemplate)
    val coidFilter: Option[SQLActionBuilderChain] = exactId.map { stringContractId =>
      val lfContractId = LfContractId.assertFromString(stringContractId)
      sql" contract_id = $lfContractId"
    }
    val limitFilter = sql" #${storage.limit(limit)}"

    val whereClause =
      List(pkgFilter, templateFilter, coidFilter)
        .foldLeft(Option.empty[SQLActionBuilderChain]) {
          case (None, Some(filter)) => Some(sql" where " ++ filter)
          case (acc, None) => acc
          case (Some(acc), Some(filter)) => Some(acc ++ sql" and " ++ filter)
        }
        .getOrElse(toSQLActionBuilderChain(sql" "))

    val contractsQuery = contractsBaseQuery ++ whereClause ++ limitFilter

    storage
      .query(contractsQuery.as[ContractInstance], functionFullName)
      .map(_.toList)
  }

  override def findWithPayload(
      contractIds: NonEmpty[Seq[LfContractId]]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, ContractInstance]] =
    storage
      .query(
        bulkLookupQuery(contractIds),
        functionFullName,
      )
      .map(_.map(c => c.contractId -> c).toMap)

  override def storeContracts(contracts: Seq[ContractInstance])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    contracts.parTraverse_(storeContract)

  private def storeContract(contract: ContractInstance)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Unit] =
    batchAggregatorInsert.run(contract).flatMap(FutureUnlessShutdown.fromTry)

  private val batchAggregatorInsert = {
    val processor = new DbBulkUpdateProcessor[ContractInstance, Unit] {
      override protected implicit def executionContext: ExecutionContext =
        DbContractStore.this.ec
      override protected def storage: DbStorage = DbContractStore.this.storage
      override def kind: String = "stored contract"
      override def logger: TracedLogger = DbContractStore.this.logger

      override def executeBatch(items: NonEmpty[Seq[Traced[ContractInstance]]])(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): FutureUnlessShutdown[Iterable[Try[Unit]]] =
        bulkUpdateWithCheck(items, "DbContractStore.insert")(traceContext, self.closeContext)

      override protected def bulkUpdateAction(items: NonEmpty[Seq[Traced[ContractInstance]]])(
          implicit batchTraceContext: TraceContext
      ): DBIOAction[Array[Int], NoStream, Effect.All] = {
        def setParams(pp: PositionedParameters)(contract: ContractInstance): Unit = {

          val packageId = contract.templateId.packageId
          val templateId = checked(String2066.tryCreate(contract.templateId.qualifiedName.toString))

          pp >> contract.contractId
          pp >> packageId
          pp >> templateId
          pp >> contract
        }

        // As we assume that the contract data has previously been authenticated against the contract id,
        // we only update those fields that are not covered by the authentication.
        val query =
          profile match {
            case _: DbStorage.Profile.Postgres =>
              """insert into par_contracts as c (
                   contract_id, package_id, template_id, instance)
                 values (?, ?, ?, ?)
                 on conflict(contract_id) do nothing"""
            case _: DbStorage.Profile.H2 =>
              """merge into par_contracts c
                 using (select cast(? as binary varying) contract_id,
                               cast(? as varchar) package_id,
                               cast(? as varchar) template_id,
                               cast(? as binary large object) instance
                               from dual) as input
                 on (c.contract_id = input.contract_id)
                 when not matched then
                  insert (contract_id, instance, package_id, template_id)
                  values (input.contract_id, input.instance, input.package_id, input.template_id)"""
          }
        DbStorage.bulkOperation(query, items.map(_.value), profile)(setParams)

      }

      override protected def onSuccessItemUpdate(item: Traced[ContractInstance]): Try[Unit] =
        Try {
          val contract = item.value
          cache.put(contract.contractId, Option(contract))
        }

      private def failWith(message: String)(implicit
          loggingContext: ErrorLoggingContext
      ): Failure[Nothing] =
        ErrorUtil.internalErrorTry(new IllegalStateException(message))

      override protected type CheckData = ContractInstance
      override protected type ItemIdentifier = LfContractId
      override protected def itemIdentifier(item: ContractInstance): ItemIdentifier =
        item.contractId
      override protected def dataIdentifier(state: CheckData): ItemIdentifier = state.contractId

      override protected def checkQuery(itemsToCheck: NonEmpty[Seq[ItemIdentifier]])(implicit
          batchTraceContext: TraceContext
      ): DbAction.ReadOnly[immutable.Iterable[CheckData]] =
        bulkLookupQuery(itemsToCheck)

      override protected def analyzeFoundData(
          item: ContractInstance,
          foundData: Option[ContractInstance],
      )(implicit
          traceContext: TraceContext
      ): Try[Unit] =
        foundData match {
          case None =>
            // the contract is not in the db
            invalidateCache(item.contractId)
            failWith(s"Failed to insert contract ${item.contractId}")
          case Some(data) =>
            if (data == item) {
              cache.put(item.contractId, Some(item))
              TryUtil.unit
            } else {
              invalidateCache(data.contractId)
              failWith(
                s"Stored contracts are immutable, but found different contract ${item.contractId}"
              )
            }
        }

      override def prettyItem: Pretty[ContractInstance] =
        ContractInstance.prettyGenContractInstance
    }

    BatchAggregator(processor, insertBatchAggregatorConfig)
  }

  override def deleteIgnoringUnknown(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import DbStorage.Implicits.BuilderChain.*
    NonEmpty.from(contractIds.toSeq) match {
      case None => FutureUnlessShutdown.unit
      case Some(cids) =>
        val inClause = DbStorage.toInClause("contract_id", cids)
        storage
          .update_(
            (sql"""delete from par_contracts where """ ++ inClause).asUpdate,
            functionFullName,
          )
          .thereafter(_ => cache.invalidateAll(contractIds))
    }
  }

  override def purge()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    storage
      .update_(
        sqlu"""delete from par_contracts""",
        functionFullName,
      )
      .thereafter(_ => cache.invalidateAll())

  override def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] =
    lookupMetadata(ids).map(_.view.mapValues(_.stakeholders).toMap)

  override def lookupSignatories(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] =
    lookupMetadata(ids).map(_.view.mapValues(_.signatories).toMap)

  def lookupMetadata(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, ContractMetadata]] =
    NonEmpty.from(ids) match {
      case None => EitherT.rightT(Map.empty)

      case Some(idsNel) =>
        EitherT(
          MonadUtil
            .parTraverseWithLimit(BatchAggregatorConfig.defaultMaximumInFlight)(
              idsNel.forgetNE.toSeq
            )(id => lookup(id).toRight(id).value)
            .map(_.collectRight)
            .map { contracts =>
              Either.cond(
                contracts.sizeCompare(ids) == 0,
                contracts
                  .map(contract => contract.contractId -> contract.metadata)
                  .toMap,
                UnknownContracts(ids -- contracts.map(_.contractId).toSet),
              )
            }
        )
    }

  override def contractCount()(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    storage.query(
      sql"select count(*) from par_contracts".as[Int].head,
      functionFullName,
    )

  override def onClosed(): Unit = {
    cache.invalidateAll()
    cache.cleanUp()
    super.onClosed()
  }
}
