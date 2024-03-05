// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{EitherT, OptionT}
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String2066
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{BatchAggregatorConfig, CacheConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.resource.DbStorage.{DbAction, SQLActionBuilderChain}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.db.DbBulkUpdateProcessor
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.EitherUtil.RichEitherIterable
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{BatchAggregator, ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, RequestCounter, checked}
import com.github.blemale.scaffeine.AsyncCache
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class DbContractStore(
    override protected val storage: DbStorage,
    domainIdIndexed: IndexedDomain,
    protocolVersion: ProtocolVersion,
    maxContractIdSqlInListSize: PositiveInt,
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
  private val domainId = domainIdIndexed.index

  override protected[store] def logger: TracedLogger = super.logger

  private implicit val storedContractGetResult: GetResult[StoredContract] = GetResult { r =>
    val contractId = r.<<[LfContractId]
    val contractInstance = r.<<[SerializableRawContractInstance]
    val metadata = r.<<[ContractMetadata]
    val ledgerCreateTime = r.<<[CantonTimestamp]
    val requestCounter = r.<<[RequestCounter]
    val creatingTransactionIdO = r.<<[Option[TransactionId]]
    val contractSalt = r.<<[Option[Salt]]

    val contract =
      SerializableContract(
        contractId,
        contractInstance,
        metadata,
        LedgerCreateTime(ledgerCreateTime),
        contractSalt,
      )
    StoredContract(contract, requestCounter, creatingTransactionIdO)
  }

  private implicit val setParameterContractMetadata: SetParameter[ContractMetadata] =
    ContractMetadata.getVersionedSetParameter(protocolVersion)

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
    sql"""select contract_id, instance, metadata, ledger_create_time, request_counter, creating_transaction_id, contract_salt
          from par_contracts"""

  private def lookupQueries(
      ids: NonEmpty[Seq[LfContractId]]
  ): immutable.Iterable[DbAction.ReadOnly[Seq[Option[StoredContract]]]] = {
    import DbStorage.Implicits.BuilderChain.*

    DbStorage.toInClauses("contract_id", ids, maxContractIdSqlInListSize).map {
      case (idGroup, inClause) =>
        (contractsBaseQuery ++ sql" where domain_id = $domainId and " ++ inClause)
          .as[StoredContract]
          .map { storedContracts =>
            val foundContracts =
              storedContracts
                .map(storedContract => (storedContract.contractId, storedContract))
                .toMap
            idGroup.map(foundContracts.get)
          }
    }
  }

  private def bulkLookupQueries(
      ids: NonEmpty[Seq[LfContractId]]
  ): immutable.Iterable[DbAction.ReadOnly[immutable.Iterable[StoredContract]]] =
    DbStorage.toInClauses_("contract_id", ids, maxContractIdSqlInListSize).map { inClause =>
      import DbStorage.Implicits.BuilderChain.*
      val query =
        contractsBaseQuery ++ sql" where domain_id = $domainId and " ++ inClause
      query.as[StoredContract]
    }

  def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[Future, StoredContract] =
    OptionT(cache.getFuture(id, _ => batchAggregatorLookup.run(id)))

  override def lookupManyUncached(
      ids: Seq[LfContractId]
  )(implicit traceContext: TraceContext): EitherT[Future, LfContractId, List[StoredContract]] =
    NonEmpty
      .from(ids)
      .map(ids =>
        EitherT(lookupManyUncachedInternal(ids).map(ids.toList.zip(_).traverse {
          case (id, contract) => contract.toRight(id)
        }))
      )
      .getOrElse(EitherT.rightT(List.empty))

  private def lookupManyUncachedInternal(
      ids: NonEmpty[Seq[LfContractId]]
  )(implicit traceContext: TraceContext) = {
    {
      storage.sequentialQueryAndCombine(lookupQueries(ids), functionFullName)(
        traceContext,
        closeContext,
      )
    }
  }

  override def find(
      filterId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[List[SerializableContract]] = {

    import DbStorage.Implicits.BuilderChain.*

    // If filter is set returns a conjunctive (`and` prepended) constraint on attribute `name`.
    // Otherwise empty sql action.
    @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
    def createConjunctiveFilter(name: String, filter: Option[String]): SQLActionBuilderChain =
      filter
        .map { f =>
          sql" and #$name " ++ (f match {
            case rs if rs.startsWith("!") => sql"= ${rs.drop(1)}" // Must be equal
            case rs if rs.startsWith("^") => sql"""like ${rs.drop(1) + "%"}""" // Starts with
            case rs => sql"""like ${"%" + rs + "%"}""" // Contains
          })
        }
        .getOrElse(sql" ")

    val where = sql" where "
    val domainConstraint = sql" domain_id = $domainId "
    val pkgFilter = createConjunctiveFilter("package_id", filterPackage)
    val templateFilter = createConjunctiveFilter("template_id", filterTemplate)
    val coidFilter = createConjunctiveFilter("contract_id", filterId)
    val limitFilter = sql" #${storage.limit(limit)}"

    val contractsQuery = contractsBaseQuery ++ where ++
      domainConstraint ++ pkgFilter ++ templateFilter ++ coidFilter ++ limitFilter

    storage
      .query(contractsQuery.as[StoredContract], functionFullName)
      .map(_.map(_.contract).toList)
  }

  override def storeCreatedContracts(
      requestCounter: RequestCounter,
      creations: Seq[WithTransactionId[SerializableContract]],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    creations.parTraverse_ { case WithTransactionId(creation, transactionId) =>
      storeContract(StoredContract.fromCreatedContract(creation, requestCounter, transactionId))
    }
  }

  override def storeDivulgedContracts(
      requestCounter: RequestCounter,
      divulgences: Seq[SerializableContract],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    divulgences.parTraverse_ { divulgence =>
      storeContract(StoredContract.fromDivulgedContract(divulgence, requestCounter))
    }
  }

  private def storeContract(
      contract: StoredContract
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit] =
    batchAggregatorInsert.run(contract).flatMap(Future.fromTry)

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
            creatingTransactionId: Option[TransactionId],
          ) = storedContract

          val template = instance.contractInstance.unversioned.template
          val packageId = template.packageId
          val templateId = checked(String2066.tryCreate(template.qualifiedName.toString))

          pp >> domainId
          pp >> contractId
          pp >> metadata
          pp >> ledgerCreateTime.ts
          pp >> requestCounter
          pp >> creatingTransactionId
          pp >> packageId
          pp >> templateId
          pp >> contractSalt
          pp >> instance
        }

        // As we assume that the contract data has previously been authenticated against the contract id,
        // we only update those fields that are not covered by the authentication.
        // Note that the instance payload cannot be updated under Oracle as updating a previously set field is problematic for Oracle when it exceeds 32KB
        // (https://support.oracle.com/knowledge/Middleware/2773919_1.html).
        val query =
          profile match {
            case _: DbStorage.Profile.Postgres =>
              """insert into par_contracts as c (
                   domain_id, contract_id, metadata,
                   ledger_create_time, request_counter, creating_transaction_id, package_id, template_id, contract_salt, instance)
                 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 on conflict(domain_id, contract_id) do update
                   set
                     request_counter = excluded.request_counter,
                     creating_transaction_id = excluded.creating_transaction_id
                   where (c.creating_transaction_id is null and (excluded.creating_transaction_id is not null or c.request_counter < excluded.request_counter)) or
                         (c.creating_transaction_id is not null and excluded.creating_transaction_id is not null and c.request_counter < excluded.request_counter)"""
            case _: DbStorage.Profile.H2 =>
              """merge into par_contracts c
                 using (select cast(? as integer) domain_id,
                               cast(? as varchar(300)) contract_id,
                               cast(? as binary large object) metadata,
                               cast(? as varchar(300)) ledger_create_time,
                               cast(? as bigint) request_counter,
                               cast(? as binary large object) creating_transaction_id,
                               cast(? as varchar(300)) package_id,
                               cast(? as varchar) template_id,
                               cast(? as binary large object) contract_salt,
                               cast(? as binary large object) instance
                               from dual) as input
                 on (c.domain_id = input.domain_id and c.contract_id = input.contract_id)
                 when matched and (
                   (c.creating_transaction_id is null and (input.creating_transaction_id is not null or c.request_counter < input.request_counter)) or
                   (c.creating_transaction_id is not null and input.creating_transaction_id is not null and c.request_counter < input.request_counter)
                 ) then
                   update set
                     request_counter = input.request_counter,
                     creating_transaction_id = input.creating_transaction_id
                 when not matched then
                  insert (domain_id, contract_id, instance, metadata, ledger_create_time,
                    request_counter, creating_transaction_id, package_id, template_id, contract_salt)
                  values (input.domain_id, input.contract_id, input.instance, input.metadata, input.ledger_create_time,
                    input.request_counter, input.creating_transaction_id, input.package_id, input.template_id, input.contract_salt)"""
            case _: DbStorage.Profile.Oracle =>
              """merge into par_contracts c
                 using (select ? domain_id,
                               ? contract_id,
                               ? metadata,
                               ? ledger_create_time,
                               ? request_counter,
                               ? creating_transaction_id,
                               ? package_id,
                               ? template_id,
                               ? contract_salt
                               from dual) input
                 on (c.domain_id = input.domain_id and c.contract_id = input.contract_id)
                 when matched then
                   update set
                     request_counter = input.request_counter,
                     creating_transaction_id = input.creating_transaction_id
                   where
                     (c.creating_transaction_id is null and (input.creating_transaction_id is not null or c.request_counter < input.request_counter)) or
                     (c.creating_transaction_id is not null and input.creating_transaction_id is not null and c.request_counter < input.request_counter)
                 when not matched then
                  insert (domain_id, contract_id, instance, metadata, ledger_create_time,
                    request_counter, creating_transaction_id, package_id, template_id, contract_salt)
                  values (input.domain_id, input.contract_id, ?, input.metadata, input.ledger_create_time,
                    input.request_counter, input.creating_transaction_id, input.package_id, input.template_id, input.contract_salt)"""
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
      // the primary key for the contracts table is (domainId, contractId)
      // since the store is handling insertion, deletion and lookup for a specific domainId, the identifier is
      // sufficient to be the contractId
      override protected type ItemIdentifier = LfContractId
      override protected def itemIdentifier(item: StoredContract): ItemIdentifier = item.contractId
      override protected def dataIdentifier(state: CheckData): ItemIdentifier = state.contractId

      override protected def checkQuery(itemsToCheck: NonEmpty[Seq[ItemIdentifier]])(implicit
          batchTraceContext: TraceContext
      ): immutable.Iterable[DbAction.ReadOnly[immutable.Iterable[CheckData]]] =
        bulkLookupQueries(itemsToCheck)

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
                case (StoredContract(_, rcItem, Some(_)), StoredContract(_, rcFound, Some(_))) =>
                  // a non-divulged contract must overwrite another non-divulged contract when its request counter is
                  // higher
                  if (rcItem > rcFound) {
                    invalidateCache(data.contractId)
                    failWith(
                      s"""Non-divulged contract ${item.contractId} with request counter $rcItem did not
                           |replace non-divulged contract ${data.contractId} with request counter $rcFound""".stripMargin
                    )
                  } else success
                case (StoredContract(_, _, Some(_)), StoredContract(_, _, None)) =>
                  // a create or transfer-in contract must overwrite a divulged contract
                  invalidateCache(data.contractId)
                  failWith(
                    s"Non-divulged contract ${item.contractId} did not replace divulged contract ${data.contractId}"
                  )
                case (StoredContract(_, _, None), StoredContract(_, _, Some(_))) =>
                  // a divulged contract should not replace a non-divulged contract
                  success
                case (StoredContract(_, rcItem, None), StoredContract(_, rcFound, None)) =>
                  // inserted and found contracts are both divulged contracts
                  if (rcItem > rcFound) {
                    invalidateCache(data.contractId)
                    failWith(
                      s"""Divulged contract ${item.contractId} with request counter $rcItem did not replace
                           |divulged contract ${data.contractId} with request counter $rcFound""".stripMargin
                    )
                  } else success
              }
            }
        }

      override def prettyItem: Pretty[StoredContract] = implicitly
    }

    BatchAggregator(processor, insertBatchAggregatorConfig)
  }

  def deleteContract(
      id: LfContractId
  )(implicit traceContext: TraceContext): EitherT[Future, UnknownContract, Unit] = {
    lookupE(id)
      .flatMap { _ =>
        EitherT.right[UnknownContract](
          storage.update_(
            sqlu"delete from par_contracts where contract_id = $id and domain_id = $domainId",
            functionFullName,
          )
        )
      }
  }
    .thereafter(_ => invalidateCache(id))

  override def deleteIgnoringUnknown(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    import DbStorage.Implicits.BuilderChain.*
    MonadUtil
      .batchedSequentialTraverse_(
        parallelism = PositiveInt.two * storage.threadsAvailableForWriting,
        chunkSize = maxContractIdSqlInListSize,
      )(contractIds.toSeq) { cids =>
        val inClause = sql"contract_id in (" ++
          cids
            .map(value => sql"$value")
            .intercalate(sql", ") ++ sql")"
        storage.update_(
          (sql"""delete from par_contracts where domain_id = $domainId and """ ++ inClause).asUpdate,
          functionFullName,
        )
      }
      .thereafter(_ => cache.synchronous().invalidateAll(contractIds))
  }

  override def deleteDivulged(
      upTo: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val query = profile match {
      case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
        sqlu"""delete from par_contracts
                 where domain_id = $domainId and request_counter <= $upTo and creating_transaction_id is null"""
      case _: DbStorage.Profile.Oracle =>
        // Here we use exactly the same expression as in idx_contracts_request_counter
        // to make sure the index is used.
        sqlu"""delete from par_contracts
                 where (case when creating_transaction_id is null then domain_id end) = $domainId and
                       (case when creating_transaction_id is null then request_counter end) <= $upTo"""
    }

    storage.update_(query, functionFullName)
  }
    .thereafter(_ => cache.synchronous().invalidateAll())

  override def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] =
    NonEmpty.from(ids) match {
      case None => EitherT.rightT(Map.empty)

      case Some(idsNel) =>
        EitherT(
          idsNel.forgetNE.toSeq
            .parTraverse(id => lookupContract(id).toRight(id).value)
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

  override def contractCount()(implicit traceContext: TraceContext): Future[Int] = {
    storage.query(sql"select count(*) from par_contracts".as[Int].head, functionFullName)
  }
}

object DbContractStore {
  final case class AbortedDueToShutdownException(message: String) extends RuntimeException(message)
}
