// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.{LengthLimitedString, String185, String300}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.SQLActionBuilderChain
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.{
  GenericStoredTopologyTransactions,
  PositiveStoredTopologyTransactions,
}
import com.digitalasset.canton.topology.store.TopologyStore.EffectiveStateChange
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
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.canton.SQLActionBuilder
import slick.jdbc.{GetResult, TransactionIsolation}
import slick.sql.SqlStreamingAction

import scala.concurrent.ExecutionContext

class DbTopologyStore[StoreId <: TopologyStoreId](
    override protected val storage: DbStorage,
    val storeId: StoreId,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    protected val maxItemsInSqlQuery: PositiveInt = PositiveInt.tryCreate(100),
)(implicit ec: ExecutionContext)
    extends TopologyStore[StoreId]
    with DbStore {

  import DbStorage.Implicits.BuilderChain.*
  import storage.api.*
  import storage.converters.*

  private implicit val getResultSignedTopologyTransaction
      : GetResult[GenericSignedTopologyTransaction] =
    SignedTopologyTransaction.createGetResultSynchronizerTopologyTransaction

  protected val transactionStoreIdName: LengthLimitedString = storeId.dbString

  def findTransactionsAndProposalsByTxHash(asOfExclusive: EffectiveTime, hashes: Set[TxHash])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] =
    if (hashes.isEmpty) FutureUnlessShutdown.pure(Seq.empty)
    else {
      logger.debug(s"Querying transactions for tx hashes $hashes as of $asOfExclusive")

      findAsOfExclusive(
        asOfExclusive,
        sql" AND (" ++ hashes
          .map(txHash => sql"tx_hash = ${txHash.hash.toLengthLimitedHexString}")
          .toList
          .intercalate(sql" OR ") ++ sql")",
        operation = "transactionsByTxHash",
      )
    }

  override def findProposalsByTxHash(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[TxHash]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] = {
    logger.debug(s"Querying proposals for tx hashes $hashes as of $asOfExclusive")

    findAsOfExclusive(
      asOfExclusive,
      sql" AND is_proposal = true AND (" ++ hashes
        .map(txHash => sql"tx_hash = ${txHash.hash.toLengthLimitedHexString}")
        .forgetNE
        .toList
        .intercalate(sql" OR ") ++ sql")",
      "proposalsByTxHash",
    )
  }

  override def findTransactionsForMapping(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[MappingHash]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] = {
    logger.debug(s"Querying transactions for mapping hashes $hashes as of $asOfExclusive")

    findAsOfExclusive(
      asOfExclusive,
      sql" AND is_proposal = false AND (" ++ hashes
        .map(mappingHash => sql"mapping_key_hash = ${mappingHash.hash.toLengthLimitedHexString}")
        .forgetNE
        .toList
        .intercalate(sql" OR ") ++ sql")",
      operation = "transactionsForMapping",
    )
  }

  /** add validated topology transaction as is to the topology transaction table
    */
  override def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removeMapping: Map[TopologyMapping.MappingHash, PositiveInt],
      removeTxs: Set[TopologyTransaction.TxHash],
      additions: Seq[GenericValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    val effectiveTs = effective.value

    val transactionRemovals = removeMapping.toList.map { case (mappingHash, serial) =>
      sql"mapping_key_hash=${mappingHash.hash.toLengthLimitedHexString} and serial_counter <= $serial"
    } ++ removeTxs.map(txHash => sql"tx_hash=${txHash.hash.toLengthLimitedHexString}")

    val updateRemovals =
      transactionRemovals.grouped(5000).toSeq.map { removals =>
        (sql"UPDATE common_topology_transactions SET valid_until = ${Some(effectiveTs)} WHERE store_id=$transactionStoreIdName AND (" ++
          removals
            .intercalate(
              sql" OR "
            ) ++ sql") AND valid_from < $effectiveTs AND valid_until is null").asUpdate
      }

    val insertAdditions =
      additions
        .grouped(1000)
        .toSeq
        .map(
          insertSignedTransaction[GenericValidatedTopologyTransaction](vtx =>
            TransactionEntry(
              sequenced,
              effective,
              Option.when(
                vtx.rejectionReason.nonEmpty || vtx.expireImmediately
              )(effective),
              vtx.transaction,
              vtx.rejectionReason.map(_.asString300),
            )
          )
        )

    storage.update_(
      DBIO
        .seq((updateRemovals ++ insertAdditions)*)
        .transactionally
        .withTransactionIsolation(TransactionIsolation.Serializable),
      operationName = "update-topology-transactions",
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
      sql"SELECT instance, sequenced, valid_from, valid_until, rejection_reason FROM common_topology_transactions WHERE store_id = $transactionStoreIdName ORDER BY id"

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

    val mappingIdFilter = getIdFilter(idFilter)
    val mappingNameSpaceFilter = getNamespaceFilter(namespaceFilter)

    val mappingTypeFilter = typeFilter(types.toSet)

    val mappingProposalsAndPreviousFilter =
      timeFilter ++ operationFilter ++ mappingIdFilter ++ mappingNameSpaceFilter ++ mappingTypeFilter ++ sql" AND is_proposal = $proposals"

    queryForTransactions(mappingProposalsAndPreviousFilter, "inspect")
  }

  override def inspectKnownParties(
      asOfExclusive: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] = {
    logger.debug(
      s"Inspecting known parties at t=$asOfExclusive with filterParty=$filterParty and filterParticipant=$filterParticipant"
    )

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
        sql" AND identifier LIKE $sqlIdentifier AND namespace LIKE $sqlNamespace"
      else sql""

    queryForTransactions(
      asOfQuery(asOfExclusive, asOfInclusive = false) ++
        sql" AND NOT is_proposal AND operation = ${TopologyChangeOp.Replace} AND ("
        // PartyToParticipant filtering
        ++ Seq(
          sql"(transaction_type = ${PartyToParticipant.code}"
            ++ conditionalAppend(filterParty, sqlPartyIdentifier, sqlPartyNS)
            ++ sql")"
        )
        ++ sql" OR "
        // SynchronizerTrustCertificate filtering
        ++ Seq(
          sql"(transaction_type = ${SynchronizerTrustCertificate.code}"
          // In SynchronizerTrustCertificate part of the filter, compare not only to participant, but also to party identifier
          // to enable searching for the admin party
            ++ conditionalAppend(filterParty, sqlPartyIdentifier, sqlPartyNS)
            ++ conditionalAppend(filterParticipant, sqlParticipantIdentifier, sqlParticipantNS)
            ++ sql")"
        )
        ++ sql")",
      operation = functionFullName,
    )
      .map(
        _.result.toSet
          .flatMap[PartyId](_.mapping match {
            case ptp: PartyToParticipant
                if filterParticipant.isEmpty || ptp.participants
                  .exists(
                    _.participantId.uid
                      .matchesPrefixes(prefixParticipantIdentifier, prefixParticipantNS)
                  ) =>
              Set(ptp.partyId)
            case cert: SynchronizerTrustCertificate
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
      types: Seq[TopologyMapping.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PositiveStoredTopologyTransactions] =
    findTransactionsBatchingUidFilter(
      asOf,
      asOfInclusive,
      isProposal,
      types.toSet,
      filterUid,
      filterNamespace,
      TopologyChangeOp.Replace.some,
    ).map(_.collectOfType[TopologyChangeOp.Replace])

  override def findFirstSequencerStateForSequencer(sequencerId: SequencerId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Option[StoredTopologyTransaction[Replace, SequencerSynchronizerState]]
  ] = {
    logger.debug(s"Querying first sequencer state for $sequencerId")

    queryForTransactions(
      // We don't expect too many MediatorSynchronizerState mappings in a single synchronizer, so fetching them all from the db
      // is acceptable and also because we don't expect to run this query frequently. We can only evaluate the
      // `mediatorId` field locally as the mediator-id is not exposed in a separate column.
      sql" AND is_proposal = false" ++
        sql" AND operation = ${TopologyChangeOp.Replace}" ++
        sql" AND transaction_type = ${SequencerSynchronizerState.code}",
      orderBy = " ORDER BY serial_counter ",
      operation = "firstSequencerState",
    ).map(
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

    queryForTransactions(
      // We don't expect too many MediatorSynchronizerState mappings in a single synchronizer, so fetching them all from the db
      // is acceptable and also because we don't expect to run this query frequently. We can only evaluate the
      // `mediatorId` field locally as the mediator-id is not exposed in a separate column.
      sql" AND is_proposal = false" ++
        sql" AND operation = ${TopologyChangeOp.Replace}" ++
        sql" AND transaction_type = ${MediatorSynchronizerState.code}",
      orderBy = " ORDER BY serial_counter ",
      operation = "firstMediatorState",
    ).map(
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

    queryForTransactions(
      sql" AND is_proposal = false" ++
        sql" AND operation = ${TopologyChangeOp.Replace}" ++
        sql" AND transaction_type = ${SynchronizerTrustCertificate.code}" ++
        sql" AND identifier = ${participant.identifier} AND namespace = ${participant.namespace}",
      limit = storage.limit(1),
      orderBy = " ORDER BY serial_counter ",
      operation = "participantFirstTrustCertificate",
    ).map(
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
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = {
    val timeFilter = sql" AND sequenced <= ${asOfInclusive.value}"
    logger.debug(s"Querying essential state as of $asOfInclusive")

    queryForTransactions(timeFilter, "essentialState", includeRejected = includeRejected)
      .map(_.asSnapshotAtMaxEffectiveTime)
  }

  override def findUpcomingEffectiveChanges(asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyStore.Change]] = {
    logger.debug(s"Querying upcoming effective changes as of $asOfInclusive")

    queryForTransactions(
      sql" AND valid_from >= $asOfInclusive ",
      orderBy = " ORDER BY valid_from",
      operation = "upcomingEffectiveChanges",
    ).map(res => res.result.map(TopologyStore.Change.selectChange).distinct)
  }

  protected def doFindCurrentAndUpcomingChangeDelays(sequencedTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[GenericStoredTopologyTransaction]] = queryForTransactions(
    sql""" AND transaction_type = ${SynchronizerParametersState.code}
             AND (valid_from >= $sequencedTime OR valid_until is NULL OR valid_until >= $sequencedTime)
             AND (valid_until is NULL or valid_from != valid_until)
             AND sequenced < $sequencedTime
             AND is_proposal = false """,
    operation = functionFullName,
  ).map(_.result)

  override def findExpiredChangeDelays(
      validUntilMinInclusive: CantonTimestamp,
      validUntilMaxExclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyStore.Change.TopologyDelay]] =
    queryForTransactions(
      sql" AND transaction_type = ${SynchronizerParametersState.code} AND $validUntilMinInclusive <= valid_until AND valid_until < $validUntilMaxExclusive AND is_proposal = false ",
      operation = functionFullName,
    ).map(_.result.mapFilter(TopologyStore.Change.selectTopologyDelay))

  override def maxTimestamp(sequencedTime: CantonTimestamp, includeRejected: Boolean)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] = {
    logger.debug(s"Querying max timestamp")

    queryForTransactions(
      sql" AND sequenced < $sequencedTime ",
      includeRejected = includeRejected,
      limit = storage.limit(1),
      orderBy = " ORDER BY valid_from DESC",
      operation = functionFullName,
    )
      .map(_.result.headOption.map(tx => (tx.sequenced, tx.validFrom)))
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

    queryForTransactions(subQuery, limit = limitQ, operation = functionFullName)
  }

  override def findStored(
      asOfExclusive: CantonTimestamp,
      transaction: GenericSignedTopologyTransaction,
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]] = {
    logger.debug(s"Querying for transaction at $asOfExclusive: $transaction")

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

    logger.debug(s"Querying for transaction $transaction with protocol version $protocolVersion")

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

    for {
      transactions <- queryForTransactions(
        sql" AND not is_proposal " ++
          sql" AND transaction_type IN (" ++ TopologyStore.initialParticipantDispatchingSet.toList
            .map(s => sql"$s")
            .intercalate(sql", ") ++ sql") ",
        operation = "participantOnboardingTransactions",
      )
      filteredTransactions = TopologyStore.filterInitialParticipantDispatchingTransactions(
        participantId,
        synchronizerId,
        transactions.result,
      )
    } yield filteredTransactions
  }

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
      val mapping = signedTx.mapping
      val transactionType = mapping.code
      val namespace = mapping.namespace
      val identifier = mapping.maybeUid.map(_.identifier).getOrElse(String185.empty)
      val serial = signedTx.serial
      val mappingHash = mapping.uniqueKey.hash.toLengthLimitedHexString
      val reason = txEntry.rejectionReason
      val txHash = signedTx.hash.hash.toLengthLimitedHexString
      val isProposal = signedTx.isProposal
      val representativeProtocolVersion = signedTx.transaction.representativeProtocolVersion
      val hashOfSignatures = signedTx.hashOfSignatures(protocolVersion).toLengthLimitedHexString

      sql"""($transactionStoreIdName, $sequencedTs, $validFrom, $validUntil, $transactionType, $namespace,
            $identifier, $mappingHash, $serial, $operation, $signedTx, $txHash, $isProposal, $reason, $representativeProtocolVersion, $hashOfSignatures)"""
    }

    (sql"""INSERT INTO common_topology_transactions (store_id, sequenced, valid_from, valid_until, transaction_type, namespace,
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
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
      filterOp: Option[TopologyChangeOp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = {
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
          )(uids)(batchedUidFilters => forwardBatch(Some(batchedUidFilters)).map(_.result))
          .map(StoredTopologyTransactions(_))
    }
  }

  private def findTransactionsSingleBatch(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Set[TopologyMapping.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
      filterOp: Option[TopologyChangeOp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = {
    val hasUidFilter = filterUid.nonEmpty || filterNamespace.nonEmpty
    // exit early if the caller produced an empty uid/namespace filter batch:
    if (hasUidFilter && filterUid.forall(_.isEmpty) && filterNamespace.forall(_.isEmpty)) {
      FutureUnlessShutdown.pure(StoredTopologyTransactions.empty)
    } else {
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

      queryForTransactions(
        timeRangeFilter ++ isProposalFilter ++ changeOpFilter ++ mappingTypeFilter ++ uidNamespaceFilter,
        operation = "singleBatch",
      )
    }
  }

  private def typeFilter(types: Set[TopologyMapping.Code]): SQLActionBuilderChain =
    if (types.isEmpty) sql""
    else
      sql" AND transaction_type IN (" ++ types.toSeq
        .map(t => sql"$t")
        .intercalate(sql", ") ++ sql")"

  private def findAsOfExclusive(
      effective: EffectiveTime,
      subQuery: SQLActionBuilder,
      operation: String,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] =
    queryForTransactions(asOfQuery(effective.value, asOfInclusive = false) ++ subQuery, operation)
      .map(_.result.map(_.transaction))

  private def findStoredSql(
      asOfExclusive: CantonTimestamp,
      transaction: GenericTopologyTransaction,
      subQuery: SQLActionBuilder = sql"",
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = {
    val mapping = transaction.mapping
    queryForTransactions(
      // Query for leading fields of `idx_common_topology_transactions` to enable use of this index
      sql" AND transaction_type = ${mapping.code} AND namespace = ${mapping.namespace} AND identifier = ${mapping.maybeUid
          .fold(String185.empty)(_.identifier)}"
        ++ sql" AND valid_from < $asOfExclusive"
        ++ sql" AND mapping_key_hash = ${mapping.uniqueKey.hash.toLengthLimitedHexString}"
        ++ sql" AND serial_counter = ${transaction.serial}"
        ++ sql" AND tx_hash = ${transaction.hash.hash.toLengthLimitedHexString}"
        ++ sql" AND operation = ${transaction.operation}"
        ++ subQuery,
      includeRejected = includeRejected,
      operation = "findStored",
    )
  }

  private def queryForTransactions(
      subQuery: SQLActionBuilder,
      operation: String,
      limit: String = "",
      orderBy: String = " ORDER BY id ",
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = {
    val query =
      sql"SELECT instance, sequenced, valid_from, valid_until, rejection_reason FROM common_topology_transactions WHERE store_id = $transactionStoreIdName" ++
        subQuery ++ (if (!includeRejected) sql" AND rejection_reason IS NULL"
                     else sql"") ++ sql" #$orderBy #$limit"

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
        s"$functionFullName-$operation",
      )
      .map(_.map { case (tx, sequencedTs, validFrom, validUntil, rejectionReason) =>
        StoredTopologyTransaction(
          SequencedTime(sequencedTs),
          EffectiveTime(validFrom),
          validUntil.map(EffectiveTime(_)),
          tx,
          rejectionReason,
        )
      })
      .map(StoredTopologyTransactions(_))
  }

  override def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = {
    val query =
      sql"SELECT watermark_ts FROM common_topology_dispatching WHERE store_id =$transactionStoreIdName"
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
                    VALUES ($transactionStoreIdName, $timestamp)
                 on conflict (store_id) do update
                  set
                    watermark_ts = $timestamp
                 """
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into common_topology_dispatching
                  using dual
                  on (store_id = $transactionStoreIdName)
                  when matched then
                    update set
                       watermark_ts = $timestamp
                  when not matched then
                    insert (store_id, watermark_ts)
                    values ($transactionStoreIdName, $timestamp)
                 """
    }
    storage.update_(query, functionFullName)
  }

  override def deleteAllData()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val deleteCommonTopologyTransactions =
      sql"delete from common_topology_transactions where store_id = $transactionStoreIdName".asUpdate
    val deleteCommonTopologyDispatching =
      sql"delete from common_topology_dispatching where store_id = $transactionStoreIdName".asUpdate

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
             AND is_proposal = false """
    queryForTransactions(
      subQuery = subQuery,
      operation = "findPositiveTransactionsForEffectiveStateChanges",
      limit = "", // all transactions are needed meeting the criteria
      orderBy = "", // not caring about the order
      includeRejected = false,
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
      idFilter: Option[String]
  ): SQLActionBuilderChain =
    idFilter match {
      case Some(value) if value.nonEmpty => sql" AND identifier like ${value + "%"}"
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
    validUntil: Option[EffectiveTime],
    signedTx: GenericSignedTopologyTransaction,
    rejectionReason: Option[String300] = None,
)

private[db] object TransactionEntry {
  def fromStoredTx(stx: GenericStoredTopologyTransaction): TransactionEntry = TransactionEntry(
    stx.sequenced,
    stx.validFrom,
    stx.validUntil,
    stx.transaction,
    rejectionReason = stx.rejectionReason,
  )
}
