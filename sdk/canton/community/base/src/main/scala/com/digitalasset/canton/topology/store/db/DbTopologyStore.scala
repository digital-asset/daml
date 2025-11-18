// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import cats.syntax.option.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.{String185, String300}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.crypto.topology.TopologyStateHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.{DbAction, SQLActionBuilderChain}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.IndexedTopologyStoreId
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.{
  GenericStoredTopologyTransactions,
  NegativeStoredTopologyTransactions,
  PositiveStoredTopologyTransactions,
}
import com.digitalasset.canton.topology.store.TopologyStore.{
  EffectiveStateChange,
  TopologyStoreDeactivations,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.TopologyMapping.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  GenericTopologyTransaction,
  TxHash,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{DBIOUtil, LoggerUtil, MonadUtil, PekkoUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import slick.jdbc.canton.SQLActionBuilder
import slick.jdbc.{GetResult, TransactionIsolation}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits.*

class DbTopologyStore[StoreId <: TopologyStoreId](
    override protected val storage: DbStorage,
    val storeId: StoreId,
    storeIndex: IndexedTopologyStoreId,
    override val protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    protected val batchingConfig: BatchingConfig,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyStore[StoreId]
    with DbStore {

  import DbStorage.Implicits.BuilderChain.*
  import storage.api.*
  import storage.converters.*

  private implicit val getResultSignedTopologyTransaction
      : GetResult[GenericSignedTopologyTransaction] =
    SignedTopologyTransaction.createGetResultSynchronizerTopologyTransaction

  def findLatestTransactionsAndProposalsByTxHash(hashes: Set[TxHash])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] =
    if (hashes.isEmpty) FutureUnlessShutdown.pure(Seq.empty)
    else {
      logger.debug(s"Querying transactions for tx hashes ${LoggerUtil.limitForLogging(hashes)}")
      MonadUtil.batchedSequentialTraverse(
        parallelism = batchingConfig.parallelism,
        chunkSize = batchingConfig.maxItemsInBatch,
      )(hashes.toSeq) { batch =>
        toStoredTopologyTransactions(
          storage.query(
            buildQueryForTransactions(
              sql" AND (" ++ batch
                .map(txHash => sql"tx_hash = ${txHash.hash}")
                .toList
                .intercalate(sql" OR ") ++ sql")"
            ),
            operationName = "transactionsByTxHash",
          )
        ).map(_.collectLatestByTxHash.result.map(_.transaction))
      }
    }

  override def findProposalsByTxHash(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[TxHash]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] = {
    logger.debug(
      s"Querying proposals for tx hashes ${LoggerUtil.limitForLogging(hashes)} as of $asOfExclusive"
    )
    MonadUtil.batchedSequentialTraverse(
      parallelism = batchingConfig.parallelism,
      chunkSize = batchingConfig.maxItemsInBatch,
    )(hashes.toSeq) { batch =>
      toSignedTopologyTransactions(
        storage.query(
          buildQueryForTransactions(
            asOfQuery(
              asOfExclusive.value,
              asOfInclusive = false,
            ) ++ sql" AND is_proposal = true AND (" ++ batch
              .map(txHash => sql"tx_hash = ${txHash.hash}")
              .toList
              .intercalate(sql" OR ") ++ sql")"
          ),
          operationName = "proposalsByTxHash",
        )
      )
    }
  }

  override def findTransactionsForMapping(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[MappingHash]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] = {
    logger.debug(
      s"Querying transactions for mapping hashes ${LoggerUtil.limitForLogging(hashes)} as of $asOfExclusive"
    )
    MonadUtil.batchedSequentialTraverse(
      parallelism = batchingConfig.parallelism,
      chunkSize = batchingConfig.maxItemsInBatch,
    )(hashes.toSeq) { batch =>
      toSignedTopologyTransactions(
        storage.query(
          buildQueryForTransactions(
            asOfQuery(
              asOfExclusive.value,
              asOfInclusive = false,
            ) ++ sql" AND is_proposal = false AND (" ++ batch
              .map(mappingHash => sql"mapping_key_hash = ${mappingHash.hash}")
              .toList
              .intercalate(sql" OR ") ++ sql")"
          ),
          operationName = "transactionsForMapping",
        )
      )
    }
  }

  /** add validated topology transaction as is to the topology transaction table
    */
  override def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removes: TopologyStoreDeactivations,
      additions: Seq[GenericValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    val effectiveTs = effective.value

    def removalSql(removals: Seq[SQLActionBuilder]) =
      (sql"UPDATE common_topology_transactions SET valid_until = ${Some(effectiveTs)} WHERE store_id=$storeIndex AND (" ++
        removals
          .intercalate(
            sql" OR "
          ) ++ sql") AND valid_from < $effectiveTs AND valid_until is null").asUpdate

    def removalForMapping(
        mapping: MappingHash,
        serialO: Option[PositiveInt],
        txHashes: Set[TxHash],
    ): SQLActionBuilderChain = if (serialO.isEmpty && txHashes.isEmpty)
      throw new IllegalArgumentException(
        s"At least one of serialO or txHashes must be defined for removal when passing $mapping"
      )
    else {
      val txConditions = (txHashes.map(txHash => sql"tx_hash=${txHash.hash}") ++ serialO
        .map(serial => sql"serial_counter <= $serial")
        .toList).toList.intercalate(sql" OR ")
      sql"(mapping_key_hash=${mapping.hash} AND (" ++ txConditions ++ sql"))"
    }

    val queries =
      DBIOUtil
        .batchedSequentialTraverse(batchingConfig.maxTopologyUpdateBatchSize)(removes.toSeq) {
          case (bulkIdx, items) =>
            logger.debug(s"Processing removal batch $bulkIdx")
            removalSql(items.map { case (mappingHash, (serialO, txHashes)) =>
              removalForMapping(mappingHash, serialO, txHashes).toActionBuilder
            })
        }
        .flatMap { _ =>
          DBIOUtil.batchedSequentialTraverse(batchingConfig.maxTopologyUpdateBatchSize)(
            additions.zipWithIndex
          ) { case (bulkIdx, items) =>
            logger.debug(s"Processing addition batch $bulkIdx")
            insertSignedTransaction[(GenericValidatedTopologyTransaction, Int)] {
              case (vtx, batchIdx) =>
                TransactionEntry(
                  sequenced,
                  effective,
                  batchIdx = batchIdx,
                  Option.when(
                    vtx.rejectionReason.nonEmpty || vtx.expireImmediately
                  )(effective),
                  vtx.transaction,
                  vtx.rejectionReason.map(_.asString300),
                )
            }(items).map(_ => ())
          }
        }

    // this will perform a fold left starting with mapping removals before inserting anything
    storage.update_(
      queries.transactionally
        .withTransactionIsolation(TransactionIsolation.Serializable),
      operationName = "update-topology-transactions",
    )

  }

  override def bulkInsert(
      initialSnapshot: GenericStoredTopologyTransactions
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    type TxWithIdx = (GenericStoredTopologyTransaction, Int)
    val (_, _, txWithIdx) =
      initialSnapshot.result.foldLeft((CantonTimestamp.MinValue, -1, Seq.empty[TxWithIdx])) {
        case ((lastValidFrom, batchIdx, acc), next) =>
          val newBatchIdx =
            if (next.validFrom.value > lastValidFrom || batchIdx == -1) 0
            else if (next.validFrom.value == lastValidFrom) batchIdx + 1
            else
              throw new IllegalStateException(
                s"Topology snapshot is unordered. Previous valid from is $lastValidFrom but next tx is $next"
              )
          (next.validFrom.value, newBatchIdx, (next, newBatchIdx) +: acc)
      }
    val prepared = txWithIdx.reverse

    val updates =
      DBIOUtil.batchedSequentialTraverse(batchingConfig.maxTopologyWriteBatchSize)(prepared) {
        case (bidx, elems) =>
          logger.debug(s"Bulk inserting batch $bidx")
          insertSignedTransaction[TxWithIdx] { case (tx, idx) =>
            TransactionEntry(
              sequenced = tx.sequenced,
              validFrom = tx.validFrom,
              batchIdx = idx,
              validUntil = tx.validUntil,
              signedTx = tx.transaction,
              rejectionReason = tx.rejectionReason,
            )
          }(elems)
      }

    storage.update_(
      updates.transactionally
        .withTransactionIsolation(TransactionIsolation.Serializable),
      operationName = "bulk-insert",
    )

  }

  @VisibleForTesting
  override protected[topology] def dumpStoreContent()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = {
    // Helper case class to produce comparable output to the InMemoryStore
    case class TopologyStoreEntry(
        transaction: GenericSignedTopologyTransaction,
        sequenced: SequencedTime,
        from: EffectiveTime,
        until: Option[EffectiveTime],
        rejected: Option[String300],
    )

    val query =
      sql"SELECT instance, sequenced, valid_from, valid_until, rejection_reason FROM common_topology_transactions WHERE store_id = $storeIndex ORDER BY id"

    val entriesF =
      storage
        .query(
          query.as[
            (
                GenericSignedTopologyTransaction,
                CantonTimestamp,
                CantonTimestamp,
                Option[CantonTimestamp],
                Option[String300],
            )
          ],
          functionFullName,
        )
        .map(_.map { case (tx, sequencedTs, validFrom, validUntil, rejectionReason) =>
          TopologyStoreEntry(
            tx,
            SequencedTime(sequencedTs),
            EffectiveTime(validFrom),
            validUntil.map(EffectiveTime(_)),
            rejectionReason,
          )
        })

    entriesF.map { entries =>
      logger.debug(
        entries
          .map(_.toString)
          .mkString("Topology Store Content[", ", ", "]")
      )
      StoredTopologyTransactions(
        entries.map(e =>
          StoredTopologyTransaction(e.sequenced, e.from, e.until, e.transaction, e.rejected)
        )
      )
    }
  }

  override def inspect(
      proposals: Boolean,
      timeQuery: TimeQuery,
      asOfExclusiveO: Option[CantonTimestamp],
      op: Option[TopologyChangeOp],
      types: Seq[TopologyMapping.Code],
      idFilter: Option[String],
      namespaceFilter: Option[String],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]] = {
    logger.debug(
      s"Inspecting store for types=$types, filter=$idFilter, time=$timeQuery, recentTimestamp=$asOfExclusiveO"
    )

    val timeFilter: SQLActionBuilderChain = timeQuery match {
      case TimeQuery.HeadState =>
        getHeadStateQuery(asOfExclusiveO)
      case TimeQuery.Snapshot(asOf) =>
        asOfQuery(asOf = asOf, asOfInclusive = false)
      case TimeQuery.Range(None, None) =>
        sql"" // The case below inserts an additional `AND` that we don't want
      case TimeQuery.Range(from, until) =>
        sql" AND " ++ ((from.toList.map(ts => sql"valid_from >= $ts") ++ until.toList.map(ts =>
          sql"valid_from <= $ts"
        ))
          .intercalate(sql" AND "))
    }

    val operationFilter = op.map(value => sql" AND operation = $value").getOrElse(sql"")

    val mappingIdFilter = getIdFilter(idFilter, exactMatch = namespaceFilter.isEmpty)
    val mappingNameSpaceFilter = getNamespaceFilter(namespaceFilter)

    val mappingTypeFilter = typeFilter(types.toSet)

    val mappingProposalsAndPreviousFilter =
      timeFilter ++ operationFilter ++ mappingIdFilter ++ mappingNameSpaceFilter ++ mappingTypeFilter ++ sql" AND is_proposal = $proposals"

    val query = buildQueryForTransactions(mappingProposalsAndPreviousFilter)
    toStoredTopologyTransactions(storage.query(query, operationName = "inspect"))
  }

  override def inspectKnownParties(
      asOfExclusive: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] = {
    logger.debug(
      s"Inspecting known parties at t=$asOfExclusive with filterParty=$filterParty and filterParticipant=$filterParticipant"
    )

    val (filterPartyIdentifier, filterPartyNamespaceO) =
      UniqueIdentifier.splitFilter(filterParty)
    val (
      filterParticipantIdentifier,
      filterParticipantNamespaceO,
    ) =
      UniqueIdentifier.splitFilter(filterParticipant)

    // conditional append avoids "like '%'" filters on empty filters
    def conditionalAppend(
        filter: String,
        filterIdentifier: String,
        filterNamespaceO: Option[String],
    ): Option[SQLActionBuilder] =
      Option
        .when[SQLActionBuilder](filter.nonEmpty)(
          getIdFilter(Some(filterIdentifier), exactMatch = filterNamespaceO.nonEmpty) ++
            getNamespaceFilter(filterNamespaceO)
        )

    val baseQuery =
      asOfQuery(asOfExclusive, asOfInclusive = false) ++
        sql" AND NOT is_proposal AND operation = ${TopologyChangeOp.Replace} AND "

    // PartyToParticipant filtering
    val ptpFilter: SQLActionBuilder =
      sql"(transaction_type = ${PartyToParticipant.code}"
        ++ conditionalAppend(
          filterParty,
          filterPartyIdentifier,
          filterPartyNamespaceO,
        ).toList ++ sql")"

    val adminPartyConditions =
      // SynchronizerTrustCertificate filtering for admin parties party
      conditionalAppend(filterParty, filterPartyIdentifier, filterPartyNamespaceO)
        .map[SQLActionBuilder](
          // prepending with TRUE, because the result of conditionalAppend starts with an AND
          sql"(TRUE " ++ _ ++ sql")"
        )
        .toList ++
        // SynchronizerTrustCertificate filtering for the participant filter
        conditionalAppend(
          filterParticipant,
          filterParticipantIdentifier,
          filterParticipantNamespaceO,
        ).map[SQLActionBuilder](
          // prepending with TRUE, because the result of conditionalAppend starts with an AND
          sql"(TRUE" ++ _ ++ sql")"
        )

    val adminPartyConditionsSql: SQLActionBuilder =
      (if (adminPartyConditions.nonEmpty)
         sql" AND (" ++ SQLActionBuilderChain.intercalate(
           adminPartyConditions,
           sql" OR ",
         ) ++ sql")"
       else sql"")

    val adminPartiesQuery: SQLActionBuilder =
      sql"(transaction_type = ${SynchronizerTrustCertificate.code}" ++ adminPartyConditionsSql ++ sql")"

    val queryWithoutIdFilter =
      baseQuery ++ sql"(" ++ Seq(ptpFilter, adminPartiesQuery).intercalate(
        sql" OR "
      ) ++ sql")"

    def inspectKnownPartiesRec(
        idOffset: Option[Long],
        partiesFoundSoFar: Vector[PartyId],
    ): FutureUnlessShutdown[Set[PartyId]] = {
      val query = buildQueryForTransactionsWithId[QueryResult](
        selectFields = TxEntryWithIdFields,
        queryWithoutIdFilter ++ idOffset.map(offset => sql" AND id > $offset").toList,
        limit = storage.limit(batchingConfig.maxItemsInBatch.value),
        // explicitly ordering by id, since we use it for paging
        orderBy = " order by id ",
        includeRejected = false,
      )
      storage.query(query, operationName = functionFullName).flatMap { rows =>
        val mappings = rows.map { case (_, (tx, _, _, _, _)) => tx.mapping }
        val parties = TopologyStore.determineValidParties(
          mappings,
          filterParty = filterParty,
          filterParticipant = filterParticipant,
          limit = limit,
        )
        val result = (partiesFoundSoFar ++ parties).distinct
        // no need to recurse, if
        // * enough parties have been found
        // * the current query didn't yield any results
        // * the current query didn't fill the batch size, therefore there are no more results
        if (
          result.sizeIs >= limit || rows.isEmpty || rows.sizeIs < batchingConfig.maxItemsInBatch.value
        ) {
          // only converting to a Set with the final result, to return the parties as they are found in id-order.
          // additionally, since we could have fetched more parties, we need to respect the user provided limit.
          FutureUnlessShutdown.pure(result.distinct.take(limit).toSet)
        } else {
          // the results are ordered by id, so we can just take the last instead of the max
          val highestIdFound = rows.lastOption.map { case (id, _) => id }
          inspectKnownPartiesRec(
            highestIdFound,
            result,
          )
        }
      }
    }

    inspectKnownPartiesRec(None, Vector.empty)
  }

  override def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMapping.Code],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]],
      filterNamespace: Option[NonEmpty[Seq[Namespace]]],
      pagination: Option[(Option[UniqueIdentifier], Int)] = None,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PositiveStoredTopologyTransactions] =
    findTransactionsBatchingUidFilter(
      asOf,
      asOfInclusive,
      isProposal,
      types.toSet,
      filterUid,
      filterNamespace,
      TopologyChangeOp.Replace.some,
      pagination,
    ).map(_.collectOfType[TopologyChangeOp.Replace])

  override def findNegativeTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMapping.Code],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]],
      filterNamespace: Option[NonEmpty[Seq[Namespace]]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[NegativeStoredTopologyTransactions] =
    findTransactionsBatchingUidFilter(
      asOf,
      asOfInclusive,
      isProposal,
      types.toSet,
      filterUid,
      filterNamespace,
      TopologyChangeOp.Remove.some,
    ).map(_.collectOfType[TopologyChangeOp.Remove])

  override def findFirstSequencerStateForSequencer(sequencerId: SequencerId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Option[StoredTopologyTransaction[Replace, SequencerSynchronizerState]]
  ] = {
    logger.debug(s"Querying first sequencer state for $sequencerId")

    val query = buildQueryForTransactions(
      // We don't expect too many SequencerSynchronizerState mappings in a single synchronizer, so fetching them all from the db
      // is acceptable and also because we don't expect to run this query frequently. We can only evaluate the
      // `sequencerId` field locally as the sequencer-id is not exposed in a separate column.
      sql" AND is_proposal = false" ++
        sql" AND operation = ${TopologyChangeOp.Replace}" ++
        sql" AND transaction_type = ${SequencerSynchronizerState.code}",
      orderBy = " ORDER BY serial_counter, valid_from ",
    )
    toStoredTopologyTransactions(
      storage.query(query, operationName = "firstSequencerState")
    )
      .map(
        _.collectOfMapping[SequencerSynchronizerState]
          .collectOfType[Replace]
          .result
          .find {
            _.mapping.allSequencers.contains(sequencerId)
          }
      )
  }

  override def findFirstMediatorStateForMediator(mediatorId: MediatorId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[StoredTopologyTransaction[Replace, MediatorSynchronizerState]]] = {
    logger.debug(s"Querying first mediator state for $mediatorId")

    val query = buildQueryForTransactions(
      // We don't expect too many MediatorSynchronizerState mappings in a single synchronizer, so fetching them all from the db
      // is acceptable and also because we don't expect to run this query frequently. We can only evaluate the
      // `mediatorId` field locally as the mediator-id is not exposed in a separate column.
      sql" AND is_proposal = false" ++
        sql" AND operation = ${TopologyChangeOp.Replace}" ++
        sql" AND transaction_type = ${MediatorSynchronizerState.code}",
      orderBy = " ORDER BY serial_counter, valid_from ",
    )
    toStoredTopologyTransactions(storage.query(query, operationName = "firstMediatorState")).map(
      _.collectOfMapping[MediatorSynchronizerState]
        .collectOfType[Replace]
        .result
        .find(tx =>
          tx.mapping.observers.contains(mediatorId) ||
            tx.mapping.active.contains(mediatorId)
        )
    )
  }

  override def findFirstTrustCertificateForParticipant(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Option[StoredTopologyTransaction[Replace, SynchronizerTrustCertificate]]
  ] = {
    logger.debug(s"Querying first trust certificate for participant $participant")

    val query = buildQueryForTransactions(
      sql" AND is_proposal = false" ++
        sql" AND operation = ${TopologyChangeOp.Replace}" ++
        sql" AND transaction_type = ${SynchronizerTrustCertificate.code}" ++
        sql" AND identifier = ${participant.identifier} AND namespace = ${participant.namespace}",
      limit = storage.limit(1),
      orderBy = " ORDER BY serial_counter, valid_from ",
    )
    toStoredTopologyTransactions(
      storage.query(query, operationName = "participantFirstTrustCertificate")
    )
      .map(
        _.collectOfMapping[SynchronizerTrustCertificate]
          .collectOfType[Replace]
          .result
          .headOption
      )
  }

  override def findEssentialStateAtSequencedTime(
      asOfInclusive: SequencedTime,
      includeRejected: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Source[GenericStoredTopologyTransaction, NotUsed] = {
    logger.debug(s"Querying essential state as of $asOfInclusive")

    val maxEffectiveTimeF =
      maxTimestamp(asOfInclusive, includeRejected = includeRejected).map(_.map {
        case (_, effective @ EffectiveTime(_)) => effective
      })

    val sourceF = maxEffectiveTimeF.map {
      case None => Source.empty
      case Some(maxEffective) =>
        findAndMapEssentialStateAtSequencedTime[QueryResult, GenericStoredTopologyTransaction](
          asOfInclusive,
          includeRejected,
          selectFields = TxEntryWithIdFields,
          transform = toStoredTopologyTransactions(_).result.map { storedTx =>
            // unset validUntil later than maxEffective, so that the node processing this
            // topology snapshot sees the transactions as they were at the effective time
            if (storedTx.validUntil.exists(_ > maxEffective)) {
              storedTx.copy(validUntil = None)
            } else storedTx
          },
          skipGenesisTransactions = false,
        )
    }

    PekkoUtil.futureSourceUS(sourceF)
  }

  private def findAndMapEssentialStateAtSequencedTime[QueryRes, Output](
      asOfInclusive: SequencedTime,
      includeRejected: Boolean,
      selectFields: SQLActionBuilder,
      transform: Vector[QueryRes] => Seq[Output],
      skipGenesisTransactions: Boolean,
  )(implicit
      getResult: GetResult[QueryResultWithId[QueryRes]],
      traceContext: TraceContext,
  ): Source[Output, NotUsed] = {
    val timeFilter = sql" AND sequenced <= ${asOfInclusive.value}"
    val skipGenesisFilter =
      if (skipGenesisTransactions)
        sql" AND sequenced > ${SignedTopologyTransaction.InitialTopologySequencingTime}"
      else sql""
    Source
      .unfoldAsync(Option.empty[Long]) { idOffset =>
        val query = buildQueryForTransactionsWithId[QueryRes](
          selectFields = selectFields,
          skipGenesisFilter ++ timeFilter ++ idOffset.map(offset => sql" AND id > $offset").toList,
          includeRejected = includeRejected,
          limit = storage.limit(batchingConfig.maxItemsInBatch.value),
          orderBy = " order by id",
        )
        storage
          .query(query, operationName = "essentialState")
          .map { rows =>
            if (rows.isEmpty) None
            else {
              val (ids, txData) = rows.unzip
              val transactions = transform(txData)
              Some(ids.lastOption -> transactions)
            }
          }
          .onShutdown(None)
      }
      .mapConcat(identity)
  }

  private val initialTopologyStateHash
      : AtomicReference[Option[PromiseUnlessShutdown[TopologyStateHash]]] =
    new AtomicReference(None)

  override def findEssentialStateHashAtSequencedTime(
      asOfInclusive: SequencedTime
  )(implicit materializer: Materializer, traceContext: TraceContext): FutureUnlessShutdown[Hash] = {

    def computeGenesisStateHash(): FutureUnlessShutdown[TopologyStateHash] = {
      logger.debug(s"Querying hashes for topology genesis state")

      FutureUnlessShutdown.outcomeF(
        findAndMapEssentialStateAtSequencedTime[Hash, Hash](
          SequencedTime(SignedTopologyTransaction.InitialTopologySequencingTime),
          includeRejected = true,
          selectFields = TxHashWithIdFields,
          transform = identity,
          skipGenesisTransactions = false,
        ).runFold(TopologyStateHash.build())(_.add(_)).map(_.finish())
      )
    }

    def useCachedOrComputeGenesisHash(): FutureUnlessShutdown[TopologyStateHash] = {
      val promise = PromiseUnlessShutdown.unsupervised[TopologyStateHash]()
      val setPromise = initialTopologyStateHash.updateAndGet {
        case None => Some(promise)
        case x => x
      }
      setPromise match {
        case Some(existingPromise) if existingPromise != promise =>
          logger.debug(
            s"Reusing existing genesis topology state hash computation"
          )
          existingPromise.futureUS
        case _ =>
          logger.debug(s"Computing genesis topology state hash at $asOfInclusive")
          promise.completeWithUS(computeGenesisStateHash()).futureUS
      }
    }

    for {
      genesisHash <- useCachedOrComputeGenesisHash()
      finalHash <- {
        if (asOfInclusive.value == SignedTopologyTransaction.InitialTopologySequencingTime) {
          FutureUnlessShutdown.pure(genesisHash)
        } else {
          logger.debug(s"Querying hashes for topology state after the genesis timestamp")

          FutureUnlessShutdown.outcomeF(
            findAndMapEssentialStateAtSequencedTime[Hash, Hash](
              asOfInclusive,
              includeRejected = true,
              selectFields = TxHashWithIdFields,
              transform = identity,
              skipGenesisTransactions = true,
            ).runFold(genesisHash.extend())(_.add(_)).map(_.finish())
          )
        }
      }
    } yield finalHash.hash
  }

  override def findUpcomingEffectiveChanges(asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyStore.Change]] = {
    logger.debug(s"Querying upcoming effective changes as of $asOfInclusive")

    val query = buildQueryForTransactions(
      sql" AND valid_from >= $asOfInclusive ",
      orderBy = " ORDER BY valid_from",
    )
    toStoredTopologyTransactions(storage.query(query, operationName = "upcomingEffectiveChanges"))
      .map(res => res.result.map(TopologyStore.Change.selectChange).distinct)
  }

  override def maxTimestamp(
      sequencedTime: SequencedTime,
      includeRejected: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] = {
    logger.debug(s"Querying max timestamp as of ${sequencedTime.value}")

    // Note: this query uses the index idx_common_topology_transactions_max_timestamp
    val query = buildQueryForMetadata[(SequencedTime, EffectiveTime)](
      "sequenced, valid_from",
      sql" AND sequenced <= ${sequencedTime.value} ",
      includeRejected = includeRejected,
      orderBy = " ORDER BY sequenced DESC",
    )
    storage
      .query(query, operationName = functionFullName)
      .map(_.headOption.map { case (sequenced @ SequencedTime(_), validFrom @ EffectiveTime(_)) =>
        (sequenced, validFrom)
      })
  }

  override def findTopologyIntervalForTimestamp(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(EffectiveTime, Option[EffectiveTime])]] = {
    logger.debug(s"Querying for topology interval for $timestamp")

    // Note: these queries use the index idx_common_topology_transactions_effective_changes
    val lowerBoundQuery = buildQueryForMetadata[EffectiveTime](
      "valid_from",
      sql" AND valid_from < $timestamp and not is_proposal",
      includeRejected = false,
      orderBy = " ORDER BY valid_from desc",
    )
    val upperBoundQuery = buildQueryForMetadata[EffectiveTime](
      "valid_from",
      sql" AND valid_from >= $timestamp and not is_proposal",
      includeRejected = false,
      orderBy = " ORDER BY valid_from asc",
    )
    val lowerBoundF = storage
      .query(lowerBoundQuery, operationName = functionFullName)
      .map(_.headOption)
    val upperBoundF = storage
      .query(upperBoundQuery, operationName = functionFullName)
      .map(_.headOption)

    for {
      lower <- lowerBoundF
      upper <- upperBoundF
    } yield lower.map(_ -> upper)
  }

  override def findDispatchingTransactionsAfter(
      timestampExclusive: CantonTimestamp,
      limitO: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = {
    val subQuery =
      sql" AND valid_from > $timestampExclusive AND (not is_proposal OR valid_until is NULL)"
    val limitQ = limitO.fold("")(storage.limit(_))

    logger.debug(s"Querying dispatching transactions after $timestampExclusive")
    val query = buildQueryForTransactions(subQuery, limit = limitQ)
    toStoredTopologyTransactions(storage.query(query, operationName = functionFullName))
  }

  override def findStored(
      asOfExclusive: CantonTimestamp,
      transaction: GenericSignedTopologyTransaction,
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]] = {
    logger.debug(s"Querying for transaction at $asOfExclusive: ${transaction.hash}")

    findStoredSql(asOfExclusive, transaction.transaction, includeRejected = includeRejected).map(
      _.result.lastOption
    )
  }

  override def findStoredForVersion(
      asOfExclusive: CantonTimestamp,
      transaction: GenericTopologyTransaction,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]] = {
    val rpv = TopologyTransaction.protocolVersionRepresentativeFor(protocolVersion)

    logger.debug(
      s"Querying for transaction ${transaction.hash} with protocol version $protocolVersion"
    )

    findStoredSql(
      asOfExclusive,
      transaction,
      subQuery = sql" AND representative_protocol_version = ${rpv.representative}",
    ).map(_.result.lastOption)
  }

  override def findParticipantOnboardingTransactions(
      participantId: ParticipantId,
      synchronizerId: SynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] = {
    logger.debug(
      s"Querying participant onboarding transactions for participant $participantId on synchronizer $synchronizerId"
    )
    val query = buildQueryForTransactions(
      sql" AND not is_proposal AND " ++
        DbStorage.toInClause("transaction_type", TopologyStore.initialParticipantDispatchingSet)
    )

    toSignedTopologyTransactions(
      storage.query(query, operationName = "participantOnboardingTransactions")
    ).map { transactions =>
      TopologyStore.filterInitialParticipantDispatchingTransactions(
        participantId,
        synchronizerId,
        transactions,
      )
    }
  }

  // Insert helper shared by bootstrap and update.
  private def insertSignedTransaction[T](toTxEntry: T => TransactionEntry)(
      transactions: Seq[T]
  ): storage.api.DBIOAction[
    Int,
    NoStream,
    Effect.Write,
  ] = {
    def sqlTransactionParameters(transaction: T) = {
      val txEntry = toTxEntry(transaction)
      val signedTx = txEntry.signedTx
      val validFrom = txEntry.validFrom.value
      val batchIdx = txEntry.batchIdx
      val validUntil = txEntry.validUntil.map(_.value)
      val sequencedTs = txEntry.sequenced.value
      val operation = signedTx.operation
      val mapping = signedTx.mapping
      val transactionType = mapping.code
      val namespace = mapping.namespace
      val identifier = mapping.maybeUid.map(_.identifier).getOrElse(String185.empty)
      val serial = signedTx.serial
      val mappingHash = mapping.uniqueKey.hash
      val reason = txEntry.rejectionReason
      val txHash = signedTx.hash.hash
      val isProposal = signedTx.isProposal
      val representativeProtocolVersion = signedTx.transaction.representativeProtocolVersion
      val hashOfSignatures = signedTx.hashOfSignatures(protocolVersion)

      sql"""($storeIndex, $sequencedTs, $validFrom, $batchIdx, $validUntil, $transactionType, $namespace,
            $identifier, $mappingHash, $serial, $operation, $signedTx, $txHash, $isProposal, $reason, $representativeProtocolVersion, $hashOfSignatures)"""
    }

    (sql"""INSERT INTO common_topology_transactions (store_id, sequenced, valid_from, batch_idx, valid_until, transaction_type, namespace,
                  identifier, mapping_key_hash, serial_counter, operation, instance, tx_hash, is_proposal, rejection_reason, representative_protocol_version, hash_of_signatures) VALUES""" ++
      transactions
        .map(sqlTransactionParameters)
        .toList
        .intercalate(sql", ")
      ++ sql" ON CONFLICT DO NOTHING" // idempotency-"conflict" based on common_topology_transactions unique constraint
    ).asUpdate
  }

  // Helper to break up large uid-filters into batches to limit the size of sql "in-clauses".
  // Fashioned to reuse lessons learned in 2.x-based DbTopologyStore
  private def findTransactionsBatchingUidFilter(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Set[TopologyMapping.Code],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]],
      filterNamespace: Option[NonEmpty[Seq[Namespace]]],
      filterOp: Option[TopologyChangeOp],
      pagination: Option[(Option[UniqueIdentifier], Int)] = None,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = {
    def forwardBatch(
        filterUidsNew: Option[NonEmpty[Seq[UniqueIdentifier]]],
        filterNamespaceNew: Option[NonEmpty[Seq[Namespace]]],
    ) =
      findTransactionsSingleBatch(
        asOf,
        asOfInclusive,
        isProposal,
        types,
        filterUidsNew,
        filterNamespaceNew,
        filterOp,
        pagination,
      )

    // Optimization: remove uid-filters made redundant by namespace filters
    val explicitUidFilters = filterUid
      .flatMap(uids =>
        NonEmpty.from(uids.filterNot(uid => filterNamespace.exists(_.contains(uid.namespace))))
      )

    // if both filters are empty, we can simply run a single batch
    if (filterNamespace.isEmpty && explicitUidFilters.isEmpty) {
      forwardBatch(None, None)
    } else {
      // split both filters into batches. we need to jump through a few hoops to go
      // from Option[NonEmpty[Seq[X]]] to
      // Seq[ // collection containing the batches
      //   Option[ // we need to retain optionality, so that we can zip the filters together and allow for a different number of uid/namespaces filter batches
      //     NonEmpty[Seq[X]] // finally the actual batch
      //   ]
      // ]
      // because grouped doesn't return NonEmpty collections.
      val chunkedUids = explicitUidFilters.flatTraverse(uids =>
        uids
          .grouped(batchingConfig.maxItemsInBatch.value)
          .toSeq
          .map[Option[NonEmpty[Seq[UniqueIdentifier]]]](NonEmpty.from)
      )
      val chunkedNamespaces =
        filterNamespace.flatTraverse(ns =>
          ns.grouped(batchingConfig.maxItemsInBatch.value)
            .toSeq
            .map[Option[NonEmpty[Seq[Namespace]]]](NonEmpty.from)
        )
      // since the filters are ORed in the query, we can simply interlace them in the same chunk.
      // if one of the filters has fewer chunks, we simply pad with None
      val chunkedFilters = chunkedUids.zipAll(chunkedNamespaces, None, None)

      MonadUtil
        .parTraverseWithLimit(
          parallelism = storage.threadsAvailableForWriting
        )(chunkedFilters) { case (batchedUidFilters, batchedNamespaceFilters) =>
          forwardBatch(
            batchedUidFilters,
            batchedNamespaceFilters,
          ).map(_.result)
        }
        .map(chunkedResult => StoredTopologyTransactions(chunkedResult.flatten))
    }
  }

  private def findTransactionsSingleBatch(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Set[TopologyMapping.Code],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]],
      filterNamespace: Option[NonEmpty[Seq[Namespace]]],
      filterOp: Option[TopologyChangeOp],
      pagination: Option[(Option[UniqueIdentifier], Int)],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = {
    val hasUidFilter = filterUid.nonEmpty || filterNamespace.nonEmpty
    val filterUidStr = filterUid.map(f => s"uids ${f.mkString(", ")}")
    val filterNamespaceStr = filterNamespace.map(f => s"namespaces ${f.mkString(", ")}")
    val filterOpStr = filterOp.map(f => s"op $f")
    val filters = filterUidStr.toList ++ filterNamespaceStr ++ filterOpStr
    val filterStr = if (filters.nonEmpty) s" with filters for ${filters.mkString("; ")}" else ""
    logger.debug(
      s"Querying transactions as of $asOf for types $types$filterStr"
    )

    val timeRangeFilter = asOfQuery(asOf, asOfInclusive)
    val isProposalFilter = sql" AND is_proposal = $isProposal"
    val changeOpFilter = filterOp.fold(sql"")(op => sql" AND operation = $op")
    val mappingTypeFilter = typeFilter(types)
    val uidNamespaceFilter =
      if (hasUidFilter) {
        val namespaceFilter = filterNamespace.toList.flatMap(_.map(ns => sql"namespace = $ns"))
        val uidFilter =
          filterUid.toList.flatten.map(uid =>
            sql"(identifier = ${uid.identifier} AND namespace = ${uid.namespace})"
          )
        sql" AND (" ++ (namespaceFilter ++ uidFilter).intercalate(sql" OR ") ++ sql")"
      } else SQLActionBuilderChain(sql"")

    val nonPaginationFilters =
      timeRangeFilter ++ isProposalFilter ++ changeOpFilter ++ mappingTypeFilter ++ uidNamespaceFilter
    val query = pagination match {
      case Some((participantStartExclusive, pageLimit)) =>
        val paginationFilter = participantStartExclusive match {
          case Some(uid) =>
            sql" AND (identifier, namespace) > (${uid.identifier.toProtoPrimitive}, ${uid.namespace.toProtoPrimitive})"
          case None =>
            sql""
        }

        buildQueryForTransactions(
          nonPaginationFilters ++ paginationFilter,
          limit = s" LIMIT $pageLimit ",
          orderBy = " ORDER BY identifier, namespace ",
        )
      case _ =>
        buildQueryForTransactions(
          nonPaginationFilters
        )
    }

    toStoredTopologyTransactions(
      storage.query(
        query,
        operationName = "singleBatch",
      )
    )
  }

  private def typeFilter(types: Set[TopologyMapping.Code]): SQLActionBuilderChain =
    NonEmpty
      .from(types)
      .map(typesNE => sql" AND " ++ DbStorage.toInClause("transaction_type", typesNE))
      .getOrElse(sql"")

  private def findStoredSql(
      asOfExclusive: CantonTimestamp,
      transaction: GenericTopologyTransaction,
      subQuery: SQLActionBuilder = sql"",
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = {
    val mapping = transaction.mapping
    toStoredTopologyTransactions(
      storage.query(
        buildQueryForTransactions(
          // Query for leading fields of `idx_common_topology_transactions` to enable use of this index
          sql" AND transaction_type = ${mapping.code} AND namespace = ${mapping.namespace} AND identifier = ${mapping.maybeUid
              .fold(String185.empty)(_.identifier)}"
            ++ sql" AND valid_from < $asOfExclusive"
            ++ sql" AND mapping_key_hash = ${mapping.uniqueKey.hash}"
            ++ sql" AND serial_counter = ${transaction.serial}"
            ++ sql" AND tx_hash = ${transaction.hash.hash}"
            ++ sql" AND operation = ${transaction.operation}"
            ++ subQuery,
          includeRejected = includeRejected,
        ),
        operationName = "findStored",
      )
    )
  }

  private type QueryResult = (
      GenericSignedTopologyTransaction,
      CantonTimestamp,
      CantonTimestamp,
      Option[CantonTimestamp],
      Option[String300],
  )

  private type QueryAction = DbAction.ReadTransactional[Vector[QueryResult]]

  private def buildQueryForTransactions(
      subQuery: SQLActionBuilder,
      limit: String = "",
      orderBy: String = " ORDER BY id ",
      includeRejected: Boolean = false,
  ): QueryAction = {
    val query =
      sql"SELECT instance, sequenced, valid_from, valid_until, rejection_reason FROM common_topology_transactions WHERE store_id = $storeIndex" ++
        subQuery ++ (if (!includeRejected) sql" AND rejection_reason IS NULL"
                     else sql"") ++ sql" #$orderBy #$limit"
    query.as[QueryResult]
  }

  private val TxEntryWithIdFields =
    sql"id, instance, sequenced, valid_from, valid_until, rejection_reason"
  private val TxHashWithIdFields = sql"id, tx_hash"

  private type QueryResultWithId[A] = (
      Long,
      A,
  )

  private type QueryActionWithId[A] = DbAction.ReadTransactional[Vector[QueryResultWithId[A]]]

  private def buildQueryForTransactionsWithId[A](
      selectFields: SQLActionBuilder,
      subQuery: SQLActionBuilder,
      limit: String,
      orderBy: String,
      includeRejected: Boolean,
  )(implicit getResult: GetResult[QueryResultWithId[A]]): QueryActionWithId[A] = {
    val query =
      sql"SELECT " ++ selectFields ++ sql" FROM common_topology_transactions WHERE store_id = $storeIndex" ++
        subQuery ++ Option
          .when(!includeRejected)(sql" AND rejection_reason IS NULL")
          .toList ++ sql" #$orderBy #$limit"
    query.as[QueryResultWithId[A]]
  }

  private def buildQueryForMetadata[T: GetResult](
      selectColumns: String,
      subQuery: SQLActionBuilder,
      orderBy: String,
      includeRejected: Boolean,
  ): DbAction.ReadTransactional[Vector[T]] = {
    val query =
      sql"SELECT #$selectColumns FROM common_topology_transactions WHERE store_id = $storeIndex" ++
        subQuery ++ (if (!includeRejected) sql" AND rejection_reason IS NULL"
                     else sql"") ++ sql" #$orderBy #${storage.limit(1)}"
    query.as[T]
  }

  private def toStoredTopologyTransactions(
      resultF: FutureUnlessShutdown[Vector[QueryResult]]
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] =
    resultF.map(result =>
      StoredTopologyTransactions(result.map {
        case (tx, sequencedTs, validFrom, validUntil, rejectionReason) =>
          StoredTopologyTransaction(
            SequencedTime(sequencedTs),
            EffectiveTime(validFrom),
            validUntil.map(EffectiveTime(_)),
            tx,
            rejectionReason,
          )
      })
    )

  private def toStoredTopologyTransactions(
      result: Vector[QueryResult]
  ): GenericStoredTopologyTransactions =
    StoredTopologyTransactions(
      result.map { case (tx, sequencedTs, validFrom, validUntil, rejectionReason) =>
        StoredTopologyTransaction(
          SequencedTime(sequencedTs),
          EffectiveTime(validFrom),
          validUntil.map(EffectiveTime(_)),
          tx,
          rejectionReason,
        )
      }
    )

  private def toSignedTopologyTransactions(
      resultF: FutureUnlessShutdown[Vector[QueryResult]]
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] =
    resultF.map(_.map { case (tx, _, _, _, _) => tx })

  override def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = {
    val query =
      sql"SELECT watermark_ts FROM common_topology_dispatching WHERE store_id =$storeIndex"
        .as[CantonTimestamp]
        .headOption
    storage.query(query, functionFullName)

  }

  override def updateDispatchingWatermark(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into common_topology_dispatching (store_id, watermark_ts)
                    VALUES ($storeIndex, $timestamp)
                 on conflict (store_id) do update
                  set
                    watermark_ts = $timestamp
                 """
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into common_topology_dispatching
                  using dual
                  on (store_id = $storeIndex)
                  when matched then
                    update set
                       watermark_ts = $timestamp
                  when not matched then
                    insert (store_id, watermark_ts)
                    values ($storeIndex, $timestamp)
                 """
    }
    storage.update_(query, functionFullName)
  }

  override def deleteAllData()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val deleteCommonTopologyTransactions =
      sql"delete from common_topology_transactions where store_id = $storeIndex".asUpdate
    val deleteCommonTopologyDispatching =
      sql"delete from common_topology_dispatching where store_id = $storeIndex".asUpdate

    storage
      .update(
        DBIO
          .sequence(Seq(deleteCommonTopologyTransactions, deleteCommonTopologyDispatching))
          .transactionally,
        functionFullName,
      )
      .map { numDeleted =>
        logger.info(
          s"Deleted ${numDeleted.sum} transaction(s) and watermark(s) from the topology store $storeId"
        )
      }
  }

  override def findEffectiveStateChanges(
      fromEffectiveInclusive: CantonTimestamp,
      filterTypes: Option[Seq[TopologyMapping.Code]],
      onlyAtEffective: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[EffectiveStateChange]] = {
    val effectiveOperator = if (onlyAtEffective) "=" else ">="

    val subQuery =
      sql""" AND (
               valid_from #$effectiveOperator $fromEffectiveInclusive
               OR valid_until #$effectiveOperator $fromEffectiveInclusive
             )
             AND (valid_until IS NULL OR valid_from != valid_until)
             AND is_proposal = false """ ++
        typeFilter(filterTypes.fold(Set.empty[TopologyMapping.Code])(_.toSet))

    toStoredTopologyTransactions(
      storage.query(
        buildQueryForTransactions(
          subQuery = subQuery,
          limit = "", // all transactions are needed meeting the criteria
          orderBy = "", // not caring about the order
          includeRejected = false,
        ),
        operationName = "findPositiveTransactionsForEffectiveStateChanges",
      )
    ).map(_.toEffectiveStateChanges(fromEffectiveInclusive, onlyAtEffective))
  }

  private def asOfQuery(asOf: CantonTimestamp, asOfInclusive: Boolean): SQLActionBuilder =
    if (asOfInclusive)
      sql" AND valid_from <= $asOf AND (valid_until is NULL OR $asOf < valid_until)"
    else
      sql" AND valid_from < $asOf AND (valid_until is NULL OR $asOf <= valid_until)"

  private def getHeadStateQuery(
      recentTimestampO: Option[CantonTimestamp]
  ): SQLActionBuilderChain = recentTimestampO match {
    case Some(value) => asOfQuery(value, asOfInclusive = false)
    case None => sql" AND valid_until is NULL"
  }

  private def getIdFilter(
      idFilter: Option[String],
      exactMatch: Boolean,
  ): SQLActionBuilderChain =
    idFilter match {
      case Some(value) if value.nonEmpty =>
        if (exactMatch) sql" AND identifier = $value"
        else sql" AND identifier LIKE ${value + "%"}"
      case _ => sql""
    }

  private def getNamespaceFilter(namespaceFilter: Option[String]): SQLActionBuilderChain =
    namespaceFilter match {
      case Some(value) if value.nonEmpty => sql" AND namespace LIKE ${value + "%"}"
      case _ => sql""
    }
}

// Helper case class to hold StoredTopologyTransaction-fields in update() providing umbrella
// values for all transactions.
private[db] final case class TransactionEntry(
    sequenced: SequencedTime,
    validFrom: EffectiveTime,
    batchIdx: Int,
    validUntil: Option[EffectiveTime],
    signedTx: GenericSignedTopologyTransaction,
    rejectionReason: Option[String300] = None,
)
