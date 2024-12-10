// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{EitherT, OptionT}
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String2066
import com.digitalasset.canton.config.{BatchAggregatorConfig, CacheConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.resource.DbStorage.{DbAction, SQLActionBuilderChain}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.DbBulkUpdateProcessor
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.EitherUtil.RichEitherIterable
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{BatchAggregator, ErrorUtil}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.digitalasset.canton.{LfPartyId, RequestCounter, checked}
import com.github.blemale.scaffeine.AsyncCache
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class DbContractStore(
    override protected val storage: DbStorage,
    protocolVersion: ReleaseProtocolVersion,
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

  private implicit val storedContractGetResult: GetResult[StoredContract] = GetResult { r =>
    val contractId = r.<<[LfContractId]
    val contractInstance = r.<<[SerializableRawContractInstance]
    val metadata = r.<<[ContractMetadata]
    val ledgerCreateTime = r.<<[CantonTimestamp]
    val requestCounter = r.<<[RequestCounter]
    val contractSalt = r.<<[Option[Salt]]

    val contract =
      SerializableContract(
        contractId,
        contractInstance,
        metadata,
        LedgerCreateTime(ledgerCreateTime),
        contractSalt,
      )
    StoredContract(contract, requestCounter)
  }

  private implicit val setParameterContractMetadata: SetParameter[ContractMetadata] =
    ContractMetadata.getVersionedSetParameter(protocolVersion.v)

  private val cache: AsyncCache[LfContractId, Option[StoredContract]] =
    cacheConfig.buildScaffeine().buildAsync[LfContractId, Option[StoredContract]]()

  private def invalidateCache(key: LfContractId): Unit =
    cache.synchronous().invalidate(key)

  // batch aggregator used for single point queries: damle will run many "lookups"
  // during interpretation. they will hit the db like a nail gun. the batch
  // aggregator will limit the number of parallel queries to the db and "batch them"
  // together. so if there is high load with a lot of interpretation happening in parallel
  // batching will kick in.
  private val batchAggregatorLookup = {
    val processor: BatchAggregator.Processor[LfContractId, Option[StoredContract]] =
      new BatchAggregator.Processor[LfContractId, Option[StoredContract]] {
        override val kind: String = "stored contract"
        override def logger: TracedLogger = DbContractStore.this.logger

        override def executeBatch(ids: NonEmpty[Seq[Traced[LfContractId]]])(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): Future[Iterable[Option[StoredContract]]] =
          lookupManyUncachedInternal(ids.map(_.value))

        override def prettyItem: Pretty[LfContractId] = implicitly
      }
    BatchAggregator(
      processor,
      dbQueryBatcherConfig,
    )
  }

  private val contractsBaseQuery =
    sql"""select contract_id, instance, metadata, ledger_create_time, request_counter, contract_salt
          from par_contracts"""

  private def lookupQuery(
      ids: NonEmpty[Seq[LfContractId]]
  ): DbAction.ReadOnly[Seq[Option[StoredContract]]] = {
    import DbStorage.Implicits.BuilderChain.*

    val inClause = DbStorage.toInClause("contract_id", ids)
    (contractsBaseQuery ++ sql" where " ++ inClause)
      .as[StoredContract]
      .map { storedContracts =>
        val foundContracts = storedContracts
          .map(storedContract => (storedContract.contractId, storedContract))
          .toMap
        ids.map(foundContracts.get)
      }
  }

  private def bulkLookupQuery(
      ids: NonEmpty[Seq[LfContractId]]
  ): DbAction.ReadOnly[immutable.Iterable[StoredContract]] = {
    val inClause = DbStorage.toInClause("contract_id", ids)
    import DbStorage.Implicits.BuilderChain.*
    val query =
      contractsBaseQuery ++ sql" where " ++ inClause
    query.as[StoredContract]
  }

  def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[Future, StoredContract] =
    OptionT(cache.getFuture(id, _ => batchAggregatorLookup.run(id)))

  override def lookupManyExistingUncached(
      ids: Seq[LfContractId]
  )(implicit traceContext: TraceContext): EitherT[Future, LfContractId, List[StoredContract]] =
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
  )(implicit traceContext: TraceContext) =
    storage.query(lookupQuery(ids), functionFullName)

  override def find(
      filterId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[List[SerializableContract]] = {

    import DbStorage.Implicits.BuilderChain.*

    // If filter is set returns a conjunctive (`and` prepended) constraint on attribute `name`.
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
    val coidFilter = createConjunctiveFilter("contract_id", filterId)
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
      .queryUnlessShutdown(contractsQuery.as[StoredContract], functionFullName)
      .map(_.map(_.contract).toList)
  }

  override def findWithPayload(
      contractIds: NonEmpty[Seq[LfContractId]],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, SerializableContract]] =
    storage
      .queryUnlessShutdown(
        bulkLookupQuery(contractIds),
        functionFullName,
      )
      .map(_.map(c => c.contractId -> c.contract).toMap)

  override def storeCreatedContracts(
      creations: Seq[(SerializableContract, RequestCounter)]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    creations.parTraverse_ { case (creation, requestCounter) =>
      storeContract(StoredContract(creation, requestCounter))
    }

  private def storeContract(
      contract: StoredContract
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.outcomeF(batchAggregatorInsert.run(contract).flatMap(Future.fromTry))

  private val batchAggregatorInsert = {
    val processor = new DbBulkUpdateProcessor[StoredContract, Unit] {
      override protected implicit def executionContext: ExecutionContext =
        DbContractStore.this.ec
      override protected def storage: DbStorage = DbContractStore.this.storage
      override def kind: String = "stored contract"
      override def logger: TracedLogger = DbContractStore.this.logger

      override def executeBatch(items: NonEmpty[Seq[Traced[StoredContract]]])(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): Future[Iterable[Try[Unit]]] =
        bulkUpdateWithCheck(items, "DbContractStore.insert")(traceContext, self.closeContext)

      override protected def bulkUpdateAction(items: NonEmpty[Seq[Traced[StoredContract]]])(implicit
          batchTraceContext: TraceContext
      ): DBIOAction[Array[Int], NoStream, Effect.All] = {
        def setParams(pp: PositionedParameters)(storedContract: StoredContract): Unit = {
          val StoredContract(
            SerializableContract(
              contractId: LfContractId,
              instance: SerializableRawContractInstance,
              metadata: ContractMetadata,
              ledgerCreateTime: LedgerCreateTime,
              contractSalt: Option[Salt],
            ),
            requestCounter: RequestCounter,
          ) = storedContract

          val template = instance.contractInstance.unversioned.template
          val packageId = template.packageId
          val templateId = checked(String2066.tryCreate(template.qualifiedName.toString))

          pp >> contractId
          pp >> metadata
          pp >> ledgerCreateTime.ts
          pp >> requestCounter
          pp >> packageId
          pp >> templateId
          pp >> contractSalt
          pp >> instance
        }

        // As we assume that the contract data has previously been authenticated against the contract id,
        // we only update those fields that are not covered by the authentication.
        val query =
          profile match {
            case _: DbStorage.Profile.Postgres =>
              """insert into par_contracts as c (
                   contract_id, metadata,
                   ledger_create_time, request_counter, package_id, template_id, contract_salt, instance)
                 values (?, ?, ?, ?, ?, ?, ?, ?)
                 on conflict(contract_id) do update
                   set
                     request_counter = excluded.request_counter
                   where (c.request_counter < excluded.request_counter)"""
            case _: DbStorage.Profile.H2 =>
              """merge into par_contracts c
                 using (select cast(? as varchar(300)) contract_id,
                               cast(? as binary large object) metadata,
                               cast(? as varchar(300)) ledger_create_time,
                               cast(? as bigint) request_counter,
                               cast(? as varchar(300)) package_id,
                               cast(? as varchar) template_id,
                               cast(? as binary large object) contract_salt,
                               cast(? as binary large object) instance
                               from dual) as input
                 on (c.contract_id = input.contract_id)
                 when matched and (c.request_counter < input.request_counter)
                 then
                   update set
                     request_counter = input.request_counter
                 when not matched then
                  insert (contract_id, instance, metadata, ledger_create_time,
                    request_counter, package_id, template_id, contract_salt)
                  values (input.contract_id, input.instance, input.metadata, input.ledger_create_time,
                    input.request_counter, input.package_id, input.template_id, input.contract_salt)"""
          }
        DbStorage.bulkOperation(query, items.map(_.value), profile)(setParams)

      }

      override protected def onSuccessItemUpdate(item: Traced[StoredContract]): Try[Unit] = Try {
        val contract = item.value
        cache.put(contract.contractId, Future(Option(contract))(executionContext))
      }

      private val success: Try[Unit] = Success(())

      private def failWith(message: String)(implicit
          loggingContext: ErrorLoggingContext
      ): Failure[Nothing] =
        ErrorUtil.internalErrorTry(new IllegalStateException(message))

      override protected type CheckData = StoredContract
      override protected type ItemIdentifier = LfContractId
      override protected def itemIdentifier(item: StoredContract): ItemIdentifier = item.contractId
      override protected def dataIdentifier(state: CheckData): ItemIdentifier = state.contractId

      override protected def checkQuery(itemsToCheck: NonEmpty[Seq[ItemIdentifier]])(implicit
          batchTraceContext: TraceContext
      ): DbAction.ReadOnly[immutable.Iterable[CheckData]] =
        bulkLookupQuery(itemsToCheck)

      override protected def analyzeFoundData(
          item: StoredContract,
          foundData: Option[StoredContract],
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
              cache.put(item.contractId, Future(Option(item))(executionContext))
              success
            } else {
              (item, data) match {
                case (StoredContract(_, rcItem), StoredContract(_, rcFound)) =>
                  // a non-divulged contract must overwrite another non-divulged contract when its request counter is
                  // higher
                  if (rcItem > rcFound) {
                    invalidateCache(data.contractId)
                    failWith(
                      s"""Non-divulged contract ${item.contractId} with request counter $rcItem did not
                           |replace non-divulged contract ${data.contractId} with request counter $rcFound""".stripMargin
                    )
                  } else success
              }
            }
        }

      override def prettyItem: Pretty[StoredContract] = implicitly
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
          .updateUnlessShutdown_(
            (sql"""delete from par_contracts where """ ++ inClause).asUpdate,
            functionFullName,
          )
          .thereafter(_ => cache.synchronous().invalidateAll(contractIds))
    }
  }

  override def purge()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    storage
      .updateUnlessShutdown_(
        sqlu"""delete from par_contracts""",
        functionFullName,
      )
      .thereafter(_ => cache.synchronous().invalidateAll())

  override def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] =
    NonEmpty.from(ids) match {
      case None => EitherT.rightT(Map.empty)

      case Some(idsNel) =>
        EitherT(
          idsNel.forgetNE.toSeq
            .parTraverse(id => FutureUnlessShutdown.outcomeF(lookupContract(id).toRight(id).value))
            .map(_.collectRight)
            .map { contracts =>
              Either.cond(
                contracts.sizeCompare(ids) == 0,
                contracts
                  .map(contract => contract.contractId -> contract.metadata.stakeholders)
                  .toMap,
                UnknownContracts(ids -- contracts.map(_.contractId).toSet),
              )
            }
        )
    }

  override def contractCount()(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    storage.queryUnlessShutdown(
      sql"select count(*) from par_contracts".as[Int].head,
      functionFullName,
    )
}
