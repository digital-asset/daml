// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{EitherT, OptionT}
import cats.implicits.{toBifunctorOps, toTraverseOps}
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.config.CantonRequireTypes.String2066
import com.digitalasset.canton.config.{BatchAggregatorConfig, CacheConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.resource.DbStorage.{DbAction, SQLActionBuilderChain}
import com.digitalasset.canton.resource.{DbParameterUtils, DbStorage, DbStore}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{BatchAggregator, ErrorUtil, MonadUtil}
import com.digitalasset.canton.{LfPartyId, checked}
import com.digitalasset.daml.lf.transaction.{CreationTime, TransactionCoder}
import com.google.protobuf.ByteString
import slick.jdbc.canton.SQLActionBuilder
import slick.jdbc.{GetResult, SetParameter}

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

  import storage.api.*
  import storage.converters.*

  private val profile = storage.profile

  override protected[store] def logger: TracedLogger = super.logger

  // TODO(#27996): optimize: evict proto deserialization from the DB threads (suggested: using a proper pekko-stream with deser stage over the batches, or do deser on client thread -but then it might be redundant-)
  private implicit def contractGetResult(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[PersistedContractInstance] = GetResult { r =>
    PersistedContractInstance(
      internalContractId = r.nextLong(),
      inst = TransactionCoder
        .decodeFatContractInstance(ByteString.copyFrom(r.<<[Array[Byte]]))
        .leftMap(e => s"Failed to decode contract instance: $e")
        .flatMap { decoded =>
          decoded.traverseCreateAt {
            case createdAt: CreationTime.CreatedAt =>
              Right(createdAt: CreationTime.CreatedAt)
            case _ =>
              Left(
                s"Creation time must be CreatedAt for contract instances with id ${decoded.contractId}"
              )
          }
        }
        .fold(
          error => throw new DbDeserializationException(s"Invalid contract instance: $error"),
          identity,
        ),
    )
  }

  private implicit val contractArraySetParameter: SetParameter[Array[ContractInstance]] =
    (cs, pp) =>
      DbParameterUtils.setArrayBytesParameterDb[ContractInstance](
        storageProfile = storage.profile,
        items = cs,
        serialize = _.encoded.toByteArray,
        pp = pp,
      )

  private implicit val contractIdArraySetParameter: SetParameter[Array[LfContractId]] =
    (coids, pp) =>
      DbParameterUtils.setArrayBytesParameterDb[LfContractId](
        storageProfile = storage.profile,
        items = coids,
        serialize = _.toBytes.toByteArray,
        pp = pp,
      )

  private val cache
      : ScaffeineCache.TunnelledAsyncCache[LfContractId, Option[PersistedContractInstance]] =
    ScaffeineCache.buildMappedAsync[LfContractId, Option[PersistedContractInstance]](
      cacheConfig.buildScaffeine(loggerFactory)
    )(logger, "DbContractStore.cache")

  private def invalidateCache(key: LfContractId): Unit =
    cache.invalidate(key)

  // batch aggregator used for single point queries: damle will run many "lookups"
  // during interpretation. they will hit the db like a nail gun. the batch
  // aggregator will limit the number of parallel queries to the db and "batch them"
  // together. so if there is high load with a lot of interpretation happening in parallel
  // batching will kick in.
  private val batchAggregatorLookup
      : BatchAggregator[LfContractId, Option[PersistedContractInstance]] = {
    val processor: BatchAggregator.Processor[LfContractId, Option[PersistedContractInstance]] =
      new BatchAggregator.Processor[LfContractId, Option[PersistedContractInstance]] {
        override val kind: String = "serializable contract"
        override def logger: TracedLogger = DbContractStore.this.logger

        override def executeBatch(ids: NonEmpty[Seq[Traced[LfContractId]]])(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): FutureUnlessShutdown[immutable.Iterable[Option[PersistedContractInstance]]] =
          storage.query(lookupQuery(ids.map(_.value)), functionFullName)(
            traceContext,
            callerCloseContext,
          )

        override def prettyItem: Pretty[LfContractId] = implicitly
      }
    BatchAggregator(
      processor,
      dbQueryBatcherConfig,
    )
  }

  private val contractsBaseQuery =
    sql"""select internal_contract_id, instance from par_contracts"""

  private def lookupQuery(
      ids: NonEmpty[Seq[LfContractId]]
  ): DbAction.ReadOnly[Seq[Option[PersistedContractInstance]]] = {
    import DbStorage.Implicits.BuilderChain.*

    val inClause = DbStorage.toInClause("contract_id", ids)
    (contractsBaseQuery ++ sql" where " ++ inClause)
      .as[PersistedContractInstance]
      .map { contracts =>
        val foundContracts = contracts
          .map(contract => (contract.asContractInstance.contractId, contract))
          .toMap
        ids.map(foundContracts.get)
      }
  }

  private def bulkLookupQuery(
      ids: NonEmpty[Seq[LfContractId]]
  ): DbAction.ReadOnly[immutable.Iterable[PersistedContractInstance]] =
    lookupQuery(ids).map(_.flatten)

  override def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, ContractInstance] =
    OptionT(lookupPersisted(id).map(_.map(_.asContractInstance)))

  override def lookupPersistedIfCached(id: LfContractId)(implicit
      traceContext: TraceContext
  ): Option[Option[PersistedContractInstance]] =
    cache.getIfPresentSync(id)

  override def lookupPersisted(id: LfContractId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PersistedContractInstance]] =
    cache.getFuture(id, _ => batchAggregatorLookup.run(id))

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
            contract.toRight(id).map(_.asContractInstance)
        }))
      )
      .getOrElse(EitherT.rightT(List.empty))

  private def lookupManyUncachedInternal(
      ids: NonEmpty[Seq[LfContractId]]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[Option[PersistedContractInstance]]] =
    storage.query(lookupQuery(ids), functionFullName)

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
      .query(contractsQuery.as[PersistedContractInstance], functionFullName)
      .map(_.map(_.asContractInstance).toList)
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
      .map(_.map(c => c.inst.contractId -> c.asContractInstance).toMap)

  override def storeContracts(contracts: Seq[ContractInstance])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, InternalContractId]] =
    NonEmpty.from(contracts.map(Traced(_))) match {
      case Some(contractsNE) =>
        batchAggregatorInsert.runMany(contractsNE).flatMap { results =>
          val contractsWithResults = contracts.zip(results)
          FutureUnlessShutdown
            .fromTry(
              MonadUtil.sequentialTraverse(contractsWithResults) { case (contract, internalId) =>
                internalId.map(id => contract.contractId -> id)
              }
            )
            .map(_.toMap)
        }
      case None => FutureUnlessShutdown.pure(Map.empty)
    }

  private type InternalContractId = Long

  private val batchAggregatorInsert: BatchAggregator[ContractInstance, Try[InternalContractId]] = {
    val processor = new BatchAggregator.Processor[ContractInstance, Try[InternalContractId]] {
      override def kind: String = "stored contract"
      override def logger: TracedLogger = DbContractStore.this.logger
      private val queryBaseName = "DbContractStore.insert"

      override def executeBatch(items: NonEmpty[Seq[Traced[ContractInstance]]])(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): FutureUnlessShutdown[immutable.Iterable[Try[InternalContractId]]] = {
        def getParameters(contract: ContractInstance): (LfContractId, String2066, String2066) = {
          val packageId = checked(String2066.tryCreate(contract.templateId.packageId))
          val templateId = checked(String2066.tryCreate(contract.templateId.qualifiedName.toString))

          (
            contract.contractId,
            packageId,
            templateId,
          )
        }

        def getBaseQuery(items: NonEmpty[Seq[ContractInstance]]): SQLActionBuilder = {
          val (contractIds, packageIds, templateIds) = items.toArray.map(getParameters).unzip3
          val instances: Array[ContractInstance] = items.toArray

          sql"""insert into par_contracts (contract_id, package_id, template_id, instance)
                 select input.contract_id, input.package_id, input.template_id, input.instance
                 from unnest($contractIds, $packageIds, $templateIds, $instances) as input(contract_id, package_id, template_id, instance)
                 """
        }

        import storage.api.*
        import DbStorage.Implicits.BuilderChain.*

        profile match {
          case _: DbStorage.Profile.Postgres =>
            val query =
              (getBaseQuery(items.map(_.value)) ++
                sql"""on conflict(contract_id) do nothing
                 returning contract_id, internal_contract_id
                 """).as[(LfContractId, InternalContractId)]
            for {
              insertedDataInternalContractIds <- storage
                .queryAndUpdate(query, functionFullName)(
                  traceContext,
                  callerCloseContext,
                )
                .map(_.toMap)
              insertedData = items.view
                .map(_.value)
                .flatMap { item =>
                  insertedDataInternalContractIds
                    .get(item.contractId)
                    .map { internalContractId =>
                      item.contractId -> PersistedContractInstance(
                        internalContractId = internalContractId,
                        inst = item.inst,
                      )
                    }
                }
                .toMap
              allContractIds = items.view.map(_.value.contractId).toSet
              notInsertedIds = allContractIds -- insertedData.keySet
              foundData <- fetchExistingContracts(notInsertedIds.toSeq)(
                traceContext,
                callerCloseContext,
              )
            } yield processBatchResults(
              items = items,
              insertedData = insertedData,
              foundData = foundData,
            )
          case _: DbStorage.Profile.H2 =>
            // H2 operates with a single connection and thus only adding the actions in one transaction is sufficient to ensure isolation
            val contracts = items.map(_.value)
            val action = (for {
              // 1. find the contracts that are already in the db
              foundData <- bulkLookupQuery(contracts.map(_.contractId)).map { persistedContracts =>
                persistedContracts
                  .map(c => c.inst.contractId -> c)
                  .toMap
              }
              // 2. insert the contracts that are not present in the db
              // Note: distinctBy is needed to handle duplicates within the same batch
              toInsert = contracts
                .filterNot(c => foundData.contains(c.contractId))
                .distinctBy(_.contractId)
              _ <- NonEmpty.from(toInsert) match {
                case Some(toInsertNE) =>
                  getBaseQuery(toInsertNE).asUpdate
                case None => DBIO.successful(0)
              }
              // 3. fetch the internal contract ids for the inserted contracts
              insertedData <-
                NonEmpty.from(toInsert) match {
                  case Some(toInsertNE) =>
                    bulkLookupQuery(toInsertNE.map(_.contractId)).map { persistedContracts =>
                      persistedContracts
                        .map(c => c.inst.contractId -> c)
                        .toMap
                    }
                  case None => DBIO.successful(Map.empty[LfContractId, PersistedContractInstance])
                }
            } yield foundData -> insertedData).transactionally
            for {
              (foundData, insertedData) <- storage.queryAndUpdate(action, s"$queryBaseName update")(
                traceContext,
                self.closeContext,
              )
            } yield processBatchResults(
              items = items,
              insertedData = insertedData,
              foundData = foundData,
            )
        }
      }

      override def prettyItem: Pretty[ContractInstance] =
        ContractInstance.prettyGenContractInstance

      private def processBatchResults(
          items: immutable.Iterable[Traced[ContractInstance]],
          insertedData: Map[LfContractId, PersistedContractInstance],
          foundData: Map[LfContractId, PersistedContractInstance],
      ): immutable.Iterable[Try[InternalContractId]] =
        items.map { item =>
          val contract = item.value
          insertedData.get(contract.contractId) match {
            case Some(persistedContractInstance) =>
              onSuccessItemUpdate(persistedContractInstance)
            case None =>
              analyzeFoundData(contract, foundData.get(contract.contractId))(item.traceContext)
          }
        }

      private def onSuccessItemUpdate(
          persistedContractInstance: PersistedContractInstance
      ): Try[InternalContractId] =
        Try {
          cache.put(
            persistedContractInstance.inst.contractId,
            Option(persistedContractInstance),
          )
          persistedContractInstance.internalContractId
        }

      private def failWith(message: String)(implicit
          loggingContext: ErrorLoggingContext
      ): Failure[Nothing] =
        ErrorUtil.internalErrorTry(new IllegalStateException(message))

      private def fetchExistingContracts(
          contractIds: Seq[LfContractId]
      )(implicit
          traceContext: TraceContext,
          closeContext: CloseContext,
      ): FutureUnlessShutdown[Map[LfContractId, PersistedContractInstance]] =
        for {
          foundInDb <- NonEmpty.from(contractIds) match {
            case Some(contractIdsNE) =>
              storage
                .query(lookupQuery(contractIdsNE), functionFullName)
                .map(_.collect { case Some(contract) => contract.inst.contractId -> contract })
            case None => FutureUnlessShutdown.pure(Seq.empty)
          }
        } yield foundInDb.toMap

      private def analyzeFoundData(
          item: ContractInstance,
          foundData: Option[PersistedContractInstance],
      )(implicit
          traceContext: TraceContext
      ): Try[InternalContractId] =
        foundData match {
          case None =>
            // the contract is not in the db
            invalidateCache(item.contractId)
            failWith(s"Failed to insert contract ${item.contractId}")
          case Some(persistedContractInstance) =>
            if (persistedContractInstance.asContractInstance == item) {
              onSuccessItemUpdate(persistedContractInstance)
            } else {
              invalidateCache(persistedContractInstance.inst.contractId)
              failWith(
                s"Stored contracts are immutable, but found different contract ${item.contractId}"
              )
            }
        }
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
        val idsSeq = idsNel.forgetNE.toSeq
        // Separate cached and uncached ids with a single cache lookup per id
        val (cachedResults, uncachedIds) = idsSeq.foldLeft(
          (Seq.empty[(LfContractId, PersistedContractInstance)], Seq.empty[LfContractId])
        ) { case ((cached, uncached), id) =>
          cache.getIfPresentSync(id) match {
            case Some(Some(persisted)) => ((id, persisted) +: cached, uncached)
            case Some(None) => (cached, uncached) // cached as not found, don't query again
            case None => (cached, id +: uncached) // not in cache, need to query
          }
        }

        val uncachedResultsF = NonEmpty.from(uncachedIds.map(Traced(_))) match {
          case Some(uncachedIdsNE) =>
            batchAggregatorLookup.runMany(uncachedIdsNE).map { results =>
              uncachedIds.zip(results).flatMap { case (id, result) =>
                // Populate cache with fetched values
                cache.put(id, result)
                result.map(persisted => (id, persisted))
              }
            }
          case None => FutureUnlessShutdown.pure(Seq.empty)
        }

        EitherT(
          uncachedResultsF.map { uncachedResults =>
            val allResults = cachedResults ++ uncachedResults
            val contracts = allResults.map { case (_, persisted) => persisted.asContractInstance }
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

  override def lookupBatchedNonCached(internalContractIds: Iterable[Long])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Long, PersistedContractInstance]] =
    NonEmpty
      .from(internalContractIds.toSeq)
      .fold(FutureUnlessShutdown.pure(Map.empty[Long, PersistedContractInstance])) {
        nonEmptyInternalContractIds =>
          import DbStorage.Implicits.BuilderChain.*

          val inClause = DbStorage.toInClause("internal_contract_id", nonEmptyInternalContractIds)
          val query =
            (contractsBaseQuery ++ sql" where " ++ inClause).as[PersistedContractInstance]
          storage
            .query(
              query,
              functionFullName,
            )
            .map(_.map(persisted => persisted.internalContractId -> persisted).toMap)
      }

  override def lookupBatchedNonCachedInternalIds(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[LfContractId, Long]] =
    NonEmpty
      .from(contractIds.toSeq)
      .fold(FutureUnlessShutdown.pure(Map.empty[LfContractId, Long])) { nonEmptyContractIds =>
        import DbStorage.Implicits.BuilderChain.*

        val inClause = DbStorage.toInClause("contract_id", nonEmptyContractIds)
        val query =
          (sql"""select contract_id, internal_contract_id from par_contracts where """ ++ inClause)
            .as[(LfContractId, Long)]
        storage
          .query(
            query,
            functionFullName,
          )
          .map(_.toMap)
      }

  // TODO(#27996): Add unit test if still needed
  override def lookupBatchedNonCachedContractIds(internalContractIds: Iterable[Long])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Long, LfContractId]] =
    NonEmpty
      .from(internalContractIds.toSeq)
      .fold(FutureUnlessShutdown.pure(Map.empty[Long, LfContractId])) { nonEmptyContractIds =>
        import DbStorage.Implicits.BuilderChain.*

        val inClause = DbStorage.toInClause("internal_contract_id", nonEmptyContractIds)
        val query =
          (sql"""select internal_contract_id, contract_id from par_contracts where """ ++ inClause)
            .as[(Long, LfContractId)]
        storage
          .query(
            query,
            functionFullName,
          )
          .map(_.toMap)
      }

}
