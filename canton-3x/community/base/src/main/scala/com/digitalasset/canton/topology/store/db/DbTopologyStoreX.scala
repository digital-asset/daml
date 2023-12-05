// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import cats.syntax.foldable.*
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.{LengthLimitedString, String185}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.resource.DbStorage.SQLActionBuilderChain
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionX.GenericStoredTopologyTransactionX
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.{
  GenericStoredTopologyTransactionsX,
  PositiveStoredTopologyTransactionsX,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyChangeOpX.Replace
import com.digitalasset.canton.topology.transaction.TopologyMappingX.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.{
  GenericTopologyTransactionX,
  TxHash,
}
import com.digitalasset.canton.topology.transaction.{
  DomainTrustCertificateX,
  MediatorDomainStateX,
  PartyToParticipantX,
  SignedTopologyTransactionX,
  TopologyChangeOpX,
  TopologyMappingX,
  TopologyTransactionX,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import slick.jdbc.GetResult
import slick.jdbc.canton.SQLActionBuilder
import slick.sql.SqlStreamingAction

import scala.concurrent.{ExecutionContext, Future}

class DbTopologyStoreX[StoreId <: TopologyStoreId](
    override protected val storage: DbStorage,
    val storeId: StoreId,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    override protected val maxItemsInSqlQuery: PositiveInt = PositiveInt.tryCreate(100),
)(implicit ec: ExecutionContext)
    extends TopologyStoreX[StoreId]
    with DbTopologyStoreCommon[StoreId]
    with DbStore {

  import DbStorage.Implicits.BuilderChain.*
  import storage.api.*
  import storage.converters.*

  private implicit val getResultSignedTopologyTransaction
      : GetResult[GenericSignedTopologyTransactionX] =
    SignedTopologyTransactionX.createGetResultDomainTopologyTransaction

  protected val transactionStoreIdName: LengthLimitedString = storeId.dbString

  protected val updatingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("topology-store-x-update")
  protected val readTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("topology-store-x-read")

  def findTransactionsByTxHash(asOfExclusive: EffectiveTime, hashes: NonEmpty[Set[TxHash]])(implicit
      traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransactionX]] =
    findAsOfExclusive(
      asOfExclusive,
      sql" AND (" ++ hashes
        .map(txHash => sql"tx_hash = ${txHash.hash.toLengthLimitedHexString}")
        .forgetNE
        .toList
        .intercalate(sql" OR ") ++ sql")",
    )

  override def findProposalsByTxHash(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[TxHash]],
  )(implicit traceContext: TraceContext): Future[Seq[GenericSignedTopologyTransactionX]] =
    findAsOfExclusive(
      asOfExclusive,
      sql" AND is_proposal = true AND (" ++ hashes
        .map(txHash => sql"tx_hash = ${txHash.hash.toLengthLimitedHexString}")
        .forgetNE
        .toList
        .intercalate(
          sql" OR "
        ) ++ sql")",
    )

  override def findTransactionsForMapping(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[MappingHash]],
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransactionX]] = findAsOfExclusive(
    asOfExclusive,
    sql" AND is_proposal = false AND (" ++ hashes
      .map(mappingHash => sql"mapping_key_hash = ${mappingHash.hash.toLengthLimitedHexString}")
      .forgetNE
      .toList
      .intercalate(sql" OR ") ++ sql")",
  )

  /** @param elements       Elements to be batched
    * @param operationName  Name of the operation
    * @param f              Create a DBIOAction from a batch
    */
  private def performBatchedDbOperation[X](
      elements: Seq[X],
      operationName: String,
      processInParallel: Boolean,
  )(
      f: Seq[X] => DBIOAction[_, NoStream, Effect.Write with Effect.Transactional]
  )(implicit traceContext: TraceContext) = if (elements.isEmpty) Future.successful(())
  else
    MonadUtil.batchedSequentialTraverse_(
      parallelism =
        if (processInParallel) PositiveInt.two * storage.threadsAvailableForWriting
        else PositiveInt.one,
      chunkSize = maxItemsInSqlQuery,
    )(elements) { elementsBatch =>
      storage.update_(
        f(elementsBatch),
        operationName = operationName,
      )
    }

  /** add validated topology transaction as is to the topology transaction table
    */
  override def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removeMapping: Set[TopologyMappingX.MappingHash],
      removeTxs: Set[TopologyTransactionX.TxHash],
      additions: Seq[GenericValidatedTopologyTransactionX],
  )(implicit traceContext: TraceContext): Future[Unit] = {

    val effectiveTs = effective.value

    val transactionRemovals = removeMapping.toList.map(mappingHash =>
      sql"mapping_key_hash=${mappingHash.hash.toLengthLimitedHexString}"
    ) ++ removeTxs.map(txHash => sql"tx_hash=${txHash.hash.toLengthLimitedHexString}")

    lazy val updateRemovals =
      (sql"UPDATE topology_transactions_x SET valid_until = ${Some(effectiveTs)} WHERE store_id=$transactionStoreIdName AND (" ++
        transactionRemovals
          .intercalate(
            sql" OR "
          ) ++ sql") AND valid_from < $effectiveTs AND valid_until is null").asUpdate

    lazy val insertAdditions =
      insertSignedTransaction[GenericValidatedTopologyTransactionX](vtx =>
        TransactionEntry(
          sequenced,
          effective,
          Option.when(
            vtx.rejectionReason.nonEmpty || vtx.expireImmediately
          )(effective),
          vtx.transaction,
          vtx.rejectionReason,
        )
      )(additions)

    updatingTime.event {
      storage.update_(
        DBIO.seq(
          if (transactionRemovals.nonEmpty) updateRemovals else DBIO.successful(0),
          if (additions.nonEmpty) insertAdditions else DBIO.successful(0),
        ),
        operationName = "update-topology-transactions",
      )
    }
  }

  // TODO(#14048) only a temporary crutch to inspect the topology state
  override def dumpStoreContent()(implicit traceContext: TraceContext): Unit = {
    // Helper case class to produce comparable output to the InMemoryStore
    case class TopologyStoreEntry(
        transaction: GenericSignedTopologyTransactionX,
        sequenced: SequencedTime,
        from: EffectiveTime,
        until: Option[EffectiveTime],
        rejected: Option[String],
    )

    val query =
      sql"SELECT instance, sequenced, valid_from, valid_until, rejection_reason FROM topology_transactions_x WHERE store_id = $transactionStoreIdName ORDER BY id"
    val entries = timeouts.io.await("dumpStoreContent")(readTime.event {
      storage
        .query(
          query.as[
            (
                GenericSignedTopologyTransactionX,
                CantonTimestamp,
                CantonTimestamp,
                Option[CantonTimestamp],
                Option[String],
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
    })

    logger.debug(
      entries
        .map(_.toString)
        .mkString("Topology Store Content[", ", ", "]")
    )
  }

  override def inspect(
      proposals: Boolean,
      timeQuery: TimeQueryX,
      recentTimestampO: Option[CantonTimestamp],
      op: Option[TopologyChangeOpX],
      typ: Option[TopologyMappingX.Code],
      idFilter: String,
      namespaceOnly: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]] = {
    val timeFilter: SQLActionBuilderChain = timeQuery match {
      case TimeQueryX.HeadState =>
        getHeadStateQuery(recentTimestampO)
      case TimeQueryX.Snapshot(asOf) =>
        asOfQuery(asOf = asOf, asOfInclusive = false)
      case TimeQueryX.Range(None, None) =>
        sql"" // The case below inserts an additional `AND` that we don't want
      case TimeQueryX.Range(from, until) =>
        sql" AND " ++ ((from.toList.map(ts => sql"valid_from >= $ts") ++ until.toList.map(ts =>
          sql"valid_from <= $ts"
        ))
          .intercalate(sql" AND "))
    }

    val operationAndPreviousFilter = op match {
      case Some(value) =>
        timeFilter ++ sql" AND operation = $value"
      case None => timeFilter
    }

    val idAndPreviousFilter = andIdFilter(operationAndPreviousFilter, idFilter, namespaceOnly)

    val mappingTypeAndPreviousFilter = typ match {
      case Some(value) => idAndPreviousFilter ++ sql" AND transaction_type = $value"
      case None => idAndPreviousFilter
    }

    val mappingProposalsAndPreviousFilter =
      mappingTypeAndPreviousFilter ++ sql" AND is_proposal = $proposals"

    queryForTransactions(mappingProposalsAndPreviousFilter)
  }

  @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
  override def inspectKnownParties(
      timestamp: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]] = {
    def splitFilterPrefixAndSql(uidFilter: String): (String, String, String, String) =
      UniqueIdentifier.splitFilter(uidFilter) match {
        case (id, ns) => (id, ns, id + "%", ns + "%")
      }

    val (prefixPartyIdentifier, prefixPartyNS, sqlPartyIdentifier, sqlPartyNS) =
      splitFilterPrefixAndSql(filterParty)
    val (
      prefixParticipantIdentifier,
      prefixParticipantNS,
      sqlParticipantIdentifier,
      sqlParticipantNS,
    ) =
      splitFilterPrefixAndSql(filterParticipant)

    // conditional append avoids "like '%'" filters on empty filters
    def conditionalAppend(filter: String, sqlIdentifier: String, sqlNamespace: String) =
      if (filter.nonEmpty)
        sql" AND identifier LIKE ${sqlIdentifier} AND namespace LIKE ${sqlNamespace}"
      else sql""

    queryForTransactions(
      asOfQuery(timestamp, asOfInclusive = false) ++
        sql" AND NOT is_proposal AND operation = ${TopologyChangeOpX.Replace} AND ("
        // PartyToParticipantX filtering
        ++ Seq(
          sql"(transaction_type = ${PartyToParticipantX.code}"
            ++ conditionalAppend(filterParty, sqlPartyIdentifier, sqlPartyNS)
            ++ sql")"
        )
        ++ sql" OR "
        // DomainTrustCertificateX filtering
        ++ Seq(
          sql"(transaction_type = ${DomainTrustCertificateX.code}"
          // In DomainTrustCertificateX part of the filter, compare not only to participant, but also to party identifier
          // to enable searching for the admin party
            ++ conditionalAppend(filterParty, sqlPartyIdentifier, sqlPartyNS)
            ++ conditionalAppend(filterParticipant, sqlParticipantIdentifier, sqlParticipantNS)
            ++ sql")"
        )
        ++ sql")",
      storage.limit(limit),
    )
      .map(
        _.result.toSet
          .flatMap[PartyId](_.transaction.transaction.mapping match {
            // TODO(#14061): post-filtering for participantId non-columns results in fewer than limit results being returned
            //  - add indexed secondary uid and/or namespace columns for participant-ids - also to support efficient lookup
            //    of "what parties a particular participant hosts" (ParticipantId => Set[PartyId])
            case ptp: PartyToParticipantX
                if filterParticipant.isEmpty || ptp.participants
                  .exists(
                    _.participantId.uid
                      .matchesPrefixes(prefixParticipantIdentifier, prefixParticipantNS)
                  ) =>
              Set(ptp.partyId)
            case cert: DomainTrustCertificateX
                if filterParty.isEmpty || cert.participantId.adminParty.uid
                  .matchesPrefixes(prefixPartyIdentifier, prefixPartyNS) =>
              Set(cert.participantId.adminParty)
            case _ => Set.empty
          })
      )
  }

  override def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMappingX.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit traceContext: TraceContext): Future[PositiveStoredTopologyTransactionsX] =
    findTransactionsBatchingUidFilter(
      asOf,
      asOfInclusive,
      isProposal,
      types.toSet,
      filterUid,
      filterNamespace,
      TopologyChangeOpX.Replace.some,
    ).map(_.collectOfType[TopologyChangeOpX.Replace])

  override def findFirstMediatorStateForMediator(mediatorId: MediatorId)(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransactionX[Replace, MediatorDomainStateX]]] =
    queryForTransactions(
      // We don't expect too many MediatorDomainStateX mappings in a single domain, so fetching them all from the db
      // is acceptable and also because we don't expect to run this query frequently. We can only evaluate the
      // `mediatorId` field locally as the mediator-id is not exposed in a separate column.
      sql" AND is_proposal = false" ++
        sql" AND operation = ${TopologyChangeOpX.Replace}" ++
        sql" AND transaction_type = ${MediatorDomainStateX.code}"
    ).map(
      _.collectOfMapping[MediatorDomainStateX]
        .collectOfType[Replace]
        .result
        .collect {
          case tx
              if tx.transaction.transaction.mapping.observers.contains(mediatorId) ||
                tx.transaction.transaction.mapping.active.contains(mediatorId) =>
            tx
        }
        .sortBy(_.transaction.transaction.serial)
        .headOption
    )

  override def findFirstTrustCertificateForParticipant(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransactionX[Replace, DomainTrustCertificateX]]] =
    queryForTransactions(
      sql" AND is_proposal = false" ++
        sql" AND operation = ${TopologyChangeOpX.Replace}" ++
        sql" AND transaction_type = ${DomainTrustCertificateX.code}" ++
        sql" AND identifier = ${participant.uid.id} AND namespace = ${participant.uid.namespace}",
      limit = storage.limit(1),
      orderBy = " ORDER BY serial_counter ",
    ).map(
      _.collectOfMapping[DomainTrustCertificateX]
        .collectOfType[Replace]
        .result
        .headOption
    )

  override def findEssentialStateForMember(member: Member, asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = {
    val timeFilter = sql" AND sequenced <= $asOfInclusive"
    queryForTransactions(timeFilter).map(_.asSnapshotAtMaxEffectiveTime)
  }

  override def bootstrap(snapshot: GenericStoredTopologyTransactionsX)(implicit
      traceContext: TraceContext
  ): Future[Unit] = updatingTime.event {
    // inserts must not be processed in parallel to keep the insertion order (as indicated by the `id` column)
    // in sync with the monotonicity of sequenced
    performBatchedDbOperation(snapshot.result, "bootstrap", processInParallel = false) { txs =>
      insertSignedTransaction[GenericStoredTopologyTransactionX](TransactionEntry.fromStoredTx)(txs)
    }
  }

  override def findUpcomingEffectiveChanges(asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TopologyStore.Change]] = queryForTransactions(
    sql" AND valid_from >= $asOfInclusive ",
    orderBy = " ORDER BY valid_from",
  ).map(res => TopologyStoreX.accumulateUpcomingEffectiveChanges(res.result))

  override def maxTimestamp()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] =
    queryForTransactions(sql"", storage.limit(1), orderBy = " ORDER BY id DESC")
      .map(_.result.headOption.map(tx => (tx.sequenced, tx.validFrom)))

  override def findDispatchingTransactionsAfter(
      timestampExclusive: CantonTimestamp,
      limitO: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = {
    val subQuery =
      sql" AND valid_from > $timestampExclusive AND (not is_proposal OR valid_until is NULL)"
    val limitQ = limitO.fold("")(storage.limit(_))
    queryForTransactions(subQuery, limitQ)
  }

  override def findStored(
      transaction: GenericSignedTopologyTransactionX,
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransactionX]] =
    findStoredSql(transaction.transaction, includeRejected = includeRejected).map(
      _.result.lastOption
    )

  override def findStoredForVersion(
      transaction: GenericTopologyTransactionX,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransactionX]] = {
    val rpv = TopologyTransactionX.protocolVersionRepresentativeFor(protocolVersion)

    findStoredSql(
      transaction,
      subQuery = sql" AND representative_protocol_version = ${rpv.representative}",
    ).map(_.result.lastOption)
  }

  override def findParticipantOnboardingTransactions(
      participantId: ParticipantId,
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransactionX]] = for {
    transactions <- FutureUnlessShutdown
      .outcomeF(
        queryForTransactions(
          sql" AND not is_proposal " ++
            sql" AND transaction_type IN (" ++ TopologyStoreX.initialParticipantDispatchingSet.toList
              .map(s => sql"$s")
              .intercalate(sql", ") ++ sql") "
        )
      )
    filteredTransactions = TopologyStoreX.filterInitialParticipantDispatchingTransactions(
      participantId,
      domainId,
      transactions.result,
    )
  } yield filteredTransactions

  // Insert helper shared by bootstrap and update.
  private def insertSignedTransaction[T](toTxEntry: T => TransactionEntry)(
      transactions: Seq[T]
  ): SqlStreamingAction[Vector[Int], Int, slick.dbio.Effect.Write]#ResultAction[
    Int,
    NoStream,
    Effect.Write,
  ] = {
    def sqlTransactionParameters(transaction: T) = {
      val txEntry = toTxEntry(transaction)
      val signedTx = txEntry.signedTx
      val validFrom = txEntry.validFrom.value
      val validUntil = txEntry.validUntil.map(_.value)
      val sequencedTs = txEntry.sequenced.value
      val operation = signedTx.operation
      val mapping = signedTx.transaction.mapping
      val transactionType = mapping.code
      val namespace = mapping.namespace
      val identifier = mapping.maybeUid.map(_.id.toLengthLimitedString).getOrElse(String185.empty)
      val serial = signedTx.transaction.serial
      val mappingHash = mapping.uniqueKey.hash.toLengthLimitedHexString
      val reason = txEntry.rejectionReason.map(_.asString1GB)
      val txHash = signedTx.transaction.hash.hash.toLengthLimitedHexString
      val isProposal = signedTx.isProposal
      val representativeProtocolVersion = signedTx.transaction.representativeProtocolVersion
      val hashOfSignatures = signedTx.hashOfSignatures.toLengthLimitedHexString

      storage.profile match {
        case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
          sql"""($transactionStoreIdName, $sequencedTs, $validFrom, $validUntil, $transactionType, $namespace,
           $identifier, $mappingHash, $serial, $operation, $signedTx, $txHash, $isProposal, $reason, $representativeProtocolVersion, $hashOfSignatures)"""
        case _: DbStorage.Profile.Oracle =>
          throw new IllegalStateException("Oracle not supported by daml 3.0/X yet")
      }
    }

    storage.profile match {
      case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
        (sql"""INSERT INTO topology_transactions_x (store_id, sequenced, valid_from, valid_until, transaction_type, namespace,
                  identifier, mapping_key_hash, serial_counter, operation, instance, tx_hash, is_proposal, rejection_reason, representative_protocol_version, hash_of_signatures) VALUES""" ++
          transactions
            .map(sqlTransactionParameters)
            .toList
            .intercalate(sql", ")
          ++ sql" ON CONFLICT DO NOTHING" // idempotency-"conflict" based on topology_transactions_x unique constraint
        ).asUpdate
      case _: DbStorage.Profile.Oracle =>
        throw new IllegalStateException("Oracle not supported by daml 3.0/X yet")
    }
  }

  // Helper to break up large uid-filters into batches to limit the size of sql "in-clauses".
  // Fashioned to reuse lessons learned in 2.x-based DbTopologyStore
  private def findTransactionsBatchingUidFilter(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Set[TopologyMappingX.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
      filterOp: Option[TopologyChangeOpX],
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = {
    def forwardBatch(filterUidsNew: Option[Seq[UniqueIdentifier]]) =
      findTransactionsSingleBatch(
        asOf,
        asOfInclusive,
        isProposal,
        types,
        filterUidsNew,
        filterNamespace,
        filterOp,
      )

    filterUid.map(
      // Optimization: remove uid-filters made redundant by namespace filters
      _.filterNot(uid => filterNamespace.exists(_.contains(uid.namespace)))
    ) match {
      case None => forwardBatch(None)
      case Some(uids) =>
        MonadUtil
          .batchedSequentialTraverse(
            parallelism = storage.threadsAvailableForWriting,
            chunkSize = maxItemsInSqlQuery,
          )(uids) { batchedUidFilters => forwardBatch(Some(batchedUidFilters)).map(_.result) }
          .map(StoredTopologyTransactionsX(_))
    }
  }

  private def findTransactionsSingleBatch(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Set[TopologyMappingX.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
      filterOp: Option[TopologyChangeOpX],
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = {
    val hasUidFilter = filterUid.nonEmpty || filterNamespace.nonEmpty
    // exit early if the caller produced an empty uid/namespace filter batch:
    if (hasUidFilter && filterUid.forall(_.isEmpty) && filterNamespace.forall(_.isEmpty)) {
      Future.successful(StoredTopologyTransactionsX.empty)
    } else {
      val timeRangeFilter = asOfQuery(asOf, asOfInclusive)
      val isProposalFilter = sql" AND is_proposal = $isProposal"
      val changeOpFilter = filterOp.fold(sql"")(op => sql" AND operation = $op")
      val mappingTypeFilter =
        sql" AND transaction_type IN (" ++ types.toSeq
          .map(t => sql"$t")
          .intercalate(sql", ") ++ sql")"
      val uidNamespaceFilter =
        if (hasUidFilter) {
          val namespaceFilter = filterNamespace.toList.flatMap(_.map(ns => sql"namespace = $ns"))
          val uidFilter =
            filterUid.toList.flatten.map(uid =>
              sql"(identifier = ${uid.id} AND namespace = ${uid.namespace})"
            )
          sql" AND (" ++ (namespaceFilter ++ uidFilter).intercalate(sql" OR ") ++ sql")"
        } else SQLActionBuilderChain(sql"")

      queryForTransactions(
        timeRangeFilter ++ isProposalFilter ++ changeOpFilter ++ mappingTypeFilter ++ uidNamespaceFilter
      )
    }
  }

  private def findAsOfExclusive(
      effective: EffectiveTime,
      subQuery: SQLActionBuilder,
  )(implicit traceContext: TraceContext): Future[Seq[GenericSignedTopologyTransactionX]] = {
    queryForTransactions(asOfQuery(effective.value, asOfInclusive = false) ++ subQuery)
      .map(_.result.map(_.transaction))
  }

  private def findStoredSql(
      transaction: GenericTopologyTransactionX,
      subQuery: SQLActionBuilder = sql"",
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = {
    val mapping = transaction.mapping
    queryForTransactions(
      // Query for leading fields of `topology_transactions_x_idx` to enable use of this index
      sql" AND transaction_type = ${mapping.code} AND namespace = ${mapping.namespace} AND identifier = ${mapping.maybeUid
          .fold(String185.empty)(_.id.toLengthLimitedString)}"
        ++ sql" AND mapping_key_hash = ${mapping.uniqueKey.hash.toLengthLimitedHexString}"
        ++ sql" AND serial_counter = ${transaction.serial}"
        ++ sql" AND tx_hash = ${transaction.hash.hash.toLengthLimitedHexString}"
        ++ sql" AND operation = ${transaction.op}"
        ++ subQuery,
      includeRejected = includeRejected,
    )
  }

  private def queryForTransactions(
      subQuery: SQLActionBuilder,
      limit: String = "",
      orderBy: String = " ORDER BY id ",
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = {
    val query =
      sql"SELECT instance, sequenced, valid_from, valid_until FROM topology_transactions_x WHERE store_id = $transactionStoreIdName" ++
        subQuery ++ (if (!includeRejected) sql" AND rejection_reason IS NULL"
                     else sql"") ++ sql" #${orderBy} #${limit}"
    readTime.event {
      storage
        .query(
          query.as[
            (
                GenericSignedTopologyTransactionX,
                CantonTimestamp,
                CantonTimestamp,
                Option[CantonTimestamp],
            )
          ],
          functionFullName,
        )
        .map(_.map { case (tx, sequencedTs, validFrom, validUntil) =>
          StoredTopologyTransactionX(
            SequencedTime(sequencedTs),
            EffectiveTime(validFrom),
            validUntil.map(EffectiveTime(_)),
            tx,
          )
        })
        .map(StoredTopologyTransactionsX(_))
    }
  }
}

// Helper case class to hold StoredTopologyTransactionX-fields in update() providing umbrella
// values for all transactions.
private[db] final case class TransactionEntry(
    sequenced: SequencedTime,
    validFrom: EffectiveTime,
    validUntil: Option[EffectiveTime],
    signedTx: GenericSignedTopologyTransactionX,
    rejectionReason: Option[TopologyTransactionRejection] = None,
)

private[db] object TransactionEntry {
  def fromStoredTx(stx: GenericStoredTopologyTransactionX): TransactionEntry = TransactionEntry(
    stx.sequenced,
    stx.validFrom,
    stx.validUntil,
    stx.transaction,
    rejectionReason = None,
  )
}
