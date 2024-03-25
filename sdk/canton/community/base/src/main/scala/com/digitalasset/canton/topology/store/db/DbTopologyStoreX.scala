// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.{LengthLimitedString, String185}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
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
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.canton.SQLActionBuilder
import slick.jdbc.{GetResult, TransactionIsolation}
import slick.sql.SqlStreamingAction

import scala.concurrent.{ExecutionContext, Future}

class DbTopologyStoreX[StoreId <: TopologyStoreId](
    override protected val storage: DbStorage,
    val storeId: StoreId,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    protected val maxItemsInSqlQuery: PositiveInt = PositiveInt.tryCreate(100),
)(implicit ec: ExecutionContext)
    extends TopologyStoreX[StoreId]
    with DbStore {

  import DbStorage.Implicits.BuilderChain.*
  import storage.api.*
  import storage.converters.*

  private implicit val getResultSignedTopologyTransaction
      : GetResult[GenericSignedTopologyTransactionX] =
    SignedTopologyTransactionX.createGetResultDomainTopologyTransaction

  protected val transactionStoreIdName: LengthLimitedString = storeId.dbString

  def findTransactionsByTxHash(asOfExclusive: EffectiveTime, hashes: Set[TxHash])(implicit
      traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransactionX]] = {

    if (hashes.isEmpty) Future.successful(Seq.empty)
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
  }

  override def findProposalsByTxHash(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[TxHash]],
  )(implicit traceContext: TraceContext): Future[Seq[GenericSignedTopologyTransactionX]] = {
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
  ): Future[Seq[GenericSignedTopologyTransactionX]] = {
    logger.debug(s"Querying proposals for mapping hashes $hashes as of $asOfExclusive")

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
      removeMapping: Map[TopologyMappingX.MappingHash, PositiveInt],
      removeTxs: Set[TopologyTransactionX.TxHash],
      additions: Seq[GenericValidatedTopologyTransactionX],
  )(implicit traceContext: TraceContext): Future[Unit] = {

    val effectiveTs = effective.value

    val transactionRemovals = removeMapping.toList.map { case (mappingHash, serial) =>
      sql"mapping_key_hash=${mappingHash.hash.toLengthLimitedHexString} and serial_counter <= $serial"
    } ++ removeTxs.map(txHash => sql"tx_hash=${txHash.hash.toLengthLimitedHexString}")

    lazy val updateRemovals =
      (sql"UPDATE common_topology_transactions SET valid_until = ${Some(effectiveTs)} WHERE store_id=$transactionStoreIdName AND (" ++
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

    storage.update_(
      DBIO
        .seq(
          if (transactionRemovals.nonEmpty) updateRemovals else DBIO.successful(0),
          if (additions.nonEmpty) insertAdditions
          else DBIO.successful(0),
        )
        .transactionally
        .withTransactionIsolation(TransactionIsolation.Serializable),
      operationName = "update-topology-transactions",
    )

  }

  @VisibleForTesting
  override protected[topology] def dumpStoreContent()(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = {
    // Helper case class to produce comparable output to the InMemoryStore
    case class TopologyStoreEntry(
        transaction: GenericSignedTopologyTransactionX,
        sequenced: SequencedTime,
        from: EffectiveTime,
        until: Option[EffectiveTime],
        rejected: Option[String],
    )

    val query =
      sql"SELECT instance, sequenced, valid_from, valid_until, rejection_reason FROM common_topology_transactions WHERE store_id = $transactionStoreIdName ORDER BY id"

    val entriesF =
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

    entriesF.map { entries =>
      logger.debug(
        entries
          .map(_.toString)
          .mkString("Topology Store Content[", ", ", "]")
      )
      StoredTopologyTransactionsX(
        entries.map(e => StoredTopologyTransactionX(e.sequenced, e.from, e.until, e.transaction))
      )
    }
  }

  override def inspect(
      proposals: Boolean,
      timeQuery: TimeQuery,
      recentTimestampO: Option[CantonTimestamp],
      op: Option[TopologyChangeOpX],
      types: Seq[TopologyMappingX.Code],
      idFilter: Option[String],
      namespaceFilter: Option[String],
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]] = {
    logger.debug(s"Inspecting store for types=$types, filter=$idFilter, time=$timeQuery")

    val timeFilter: SQLActionBuilderChain = timeQuery match {
      case TimeQuery.HeadState =>
        getHeadStateQuery(recentTimestampO)
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

  @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
  override def inspectKnownParties(
      timestamp: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]] = {
    logger.debug(
      s"Inspecting known parties at t=$timestamp with filterParty=$filterParty and filterParticipant=$filterParticipant"
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
          .flatMap[PartyId](_.mapping match {
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

  override def findFirstSequencerStateForSequencer(sequencerId: SequencerId)(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransactionX[Replace, SequencerDomainStateX]]] = {
    logger.debug(s"Querying first sequencer state for $sequencerId")

    queryForTransactions(
      // We don't expect too many MediatorDomainStateX mappings in a single domain, so fetching them all from the db
      // is acceptable and also because we don't expect to run this query frequently. We can only evaluate the
      // `mediatorId` field locally as the mediator-id is not exposed in a separate column.
      sql" AND is_proposal = false" ++
        sql" AND operation = ${TopologyChangeOpX.Replace}" ++
        sql" AND transaction_type = ${SequencerDomainStateX.code}",
      operation = "firstSequencerState",
    ).map(
      _.collectOfMapping[SequencerDomainStateX]
        .collectOfType[Replace]
        .result
        .filter {
          _.mapping.allSequencers.contains(sequencerId)
        }
        .sortBy(_.serial)
        .headOption
    )
  }

  override def findFirstMediatorStateForMediator(mediatorId: MediatorId)(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransactionX[Replace, MediatorDomainStateX]]] = {
    logger.debug(s"Querying first mediator state for $mediatorId")

    queryForTransactions(
      // We don't expect too many MediatorDomainStateX mappings in a single domain, so fetching them all from the db
      // is acceptable and also because we don't expect to run this query frequently. We can only evaluate the
      // `mediatorId` field locally as the mediator-id is not exposed in a separate column.
      sql" AND is_proposal = false" ++
        sql" AND operation = ${TopologyChangeOpX.Replace}" ++
        sql" AND transaction_type = ${MediatorDomainStateX.code}",
      operation = "firstMediatorState",
    ).map(
      _.collectOfMapping[MediatorDomainStateX]
        .collectOfType[Replace]
        .result
        .collect {
          case tx
              if tx.mapping.observers.contains(mediatorId) ||
                tx.mapping.active.contains(mediatorId) =>
            tx
        }
        .sortBy(_.serial)
        .headOption
    )
  }

  override def findFirstTrustCertificateForParticipant(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransactionX[Replace, DomainTrustCertificateX]]] = {
    logger.debug(s"Querying first trust certificate for participant $participant")

    queryForTransactions(
      sql" AND is_proposal = false" ++
        sql" AND operation = ${TopologyChangeOpX.Replace}" ++
        sql" AND transaction_type = ${DomainTrustCertificateX.code}" ++
        sql" AND identifier = ${participant.uid.id} AND namespace = ${participant.uid.namespace}",
      limit = storage.limit(1),
      orderBy = " ORDER BY serial_counter ",
      operation = "participantFirstTrustCertificate",
    ).map(
      _.collectOfMapping[DomainTrustCertificateX]
        .collectOfType[Replace]
        .result
        .headOption
    )
  }

  override def findEssentialStateAtSequencedTime(
      asOfInclusive: SequencedTime,
      excludeMappings: Seq[TopologyMappingX.Code],
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = {
    val timeFilter = sql" AND sequenced <= ${asOfInclusive.value}"
    val mappingFilter = excludeMapping(excludeMappings.toSet)
    logger.debug(s"Querying essential state as of asOfInclusive")

    queryForTransactions(timeFilter ++ mappingFilter, "essentialState").map(
      _.asSnapshotAtMaxEffectiveTime.retainAuthorizedHistoryAndEffectiveProposals
    )
  }

  override def bootstrap(snapshot: GenericStoredTopologyTransactionsX)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    // inserts must not be processed in parallel to keep the insertion order (as indicated by the `id` column)
    // in sync with the monotonicity of sequenced
    performBatchedDbOperation(snapshot.result, "bootstrap", processInParallel = false) { txs =>
      insertSignedTransaction[GenericStoredTopologyTransactionX](TransactionEntry.fromStoredTx)(txs)
    }

  override def findUpcomingEffectiveChanges(asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TopologyStoreX.Change]] = {
    logger.debug(s"Querying upcoming effective changes as of $asOfInclusive")

    queryForTransactions(
      sql" AND valid_from >= $asOfInclusive ",
      orderBy = " ORDER BY valid_from",
      operation = "upcomingEffectiveChanges",
    ).map(res => TopologyStoreX.accumulateUpcomingEffectiveChanges(res.result))
  }

  override def maxTimestamp()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] = {
    logger.debug(s"Querying max timestamp")

    queryForTransactions(sql"", storage.limit(1), orderBy = " ORDER BY id DESC")
      .map(_.result.headOption.map(tx => (tx.sequenced, tx.validFrom)))
  }

  override def findDispatchingTransactionsAfter(
      timestampExclusive: CantonTimestamp,
      limitO: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = {
    val subQuery =
      sql" AND valid_from > $timestampExclusive AND (not is_proposal OR valid_until is NULL)"
    val limitQ = limitO.fold("")(storage.limit(_))

    logger.debug(s"Querying dispatching transactions after $timestampExclusive")

    queryForTransactions(subQuery, limitQ)
  }

  override def findStored(
      asOfExclusive: CantonTimestamp,
      transaction: GenericSignedTopologyTransactionX,
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransactionX]] = {
    logger.debug(s"Querying for transaction at $asOfExclusive: $transaction")

    findStoredSql(asOfExclusive, transaction.transaction, includeRejected = includeRejected).map(
      _.result.lastOption
    )
  }

  override def findStoredForVersion(
      asOfExclusive: CantonTimestamp,
      transaction: GenericTopologyTransactionX,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransactionX]] = {
    val rpv = TopologyTransactionX.protocolVersionRepresentativeFor(protocolVersion)

    logger.debug(s"Querying for transaction $transaction with protocol version $protocolVersion")

    findStoredSql(
      asOfExclusive,
      transaction,
      subQuery = sql" AND representative_protocol_version = ${rpv.representative}",
    ).map(_.result.lastOption)
  }

  override def findParticipantOnboardingTransactions(
      participantId: ParticipantId,
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransactionX]] = {
    logger.debug(
      s"Querying participant onboarding transactions for participant $participantId on domain $domainId"
    )

    for {
      transactions <- FutureUnlessShutdown
        .outcomeF(
          queryForTransactions(
            sql" AND not is_proposal " ++
              sql" AND transaction_type IN (" ++ TopologyStoreX.initialParticipantDispatchingSet.toList
                .map(s => sql"$s")
                .intercalate(sql", ") ++ sql") ",
            operation = "participantOnboardingTransactions",
          )
        )
      filteredTransactions = TopologyStoreX.filterInitialParticipantDispatchingTransactions(
        participantId,
        domainId,
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
      val identifier = mapping.maybeUid.map(_.id.toLengthLimitedString).getOrElse(String185.empty)
      val serial = signedTx.serial
      val mappingHash = mapping.uniqueKey.hash.toLengthLimitedHexString
      val reason = txEntry.rejectionReason.map(_.asString1GB)
      val txHash = signedTx.hash.hash.toLengthLimitedHexString
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

    // TODO(#14061): Decide whether we want additional indices by mapping_key_hash and tx_hash (e.g. for update/removal and lookups)
    // TODO(#14061): Come up with columns/indexing for efficient ParticipantId => Seq[PartyId] lookup
    // TODO(#12390) should mapping_key_hash rather be tx_hash?
    storage.profile match {
      case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
        (sql"""INSERT INTO common_topology_transactions (store_id, sequenced, valid_from, valid_until, transaction_type, namespace,
                  identifier, mapping_key_hash, serial_counter, operation, instance, tx_hash, is_proposal, rejection_reason, representative_protocol_version, hash_of_signatures) VALUES""" ++
          transactions
            .map(sqlTransactionParameters)
            .toList
            .intercalate(sql", ")
          ++ sql" ON CONFLICT DO NOTHING" // idempotency-"conflict" based on common_topology_transactions unique constraint
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
      logger.debug(s"Querying transactions as of $asOf for types $types")

      val timeRangeFilter = asOfQuery(asOf, asOfInclusive)
      val isProposalFilter = sql" AND is_proposal = $isProposal"
      val changeOpFilter = filterOp.fold(sql"")(op => sql" AND operation = $op")
      val mappingTypeFilter = typeFilter(types)
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
        timeRangeFilter ++ isProposalFilter ++ changeOpFilter ++ mappingTypeFilter ++ uidNamespaceFilter,
        operation = "singleBatch",
      )
    }
  }

  private def typeFilter(types: Set[TopologyMappingX.Code]): SQLActionBuilderChain = {
    if (types.isEmpty) sql""
    else
      sql" AND transaction_type IN (" ++ types.toSeq
        .map(t => sql"$t")
        .intercalate(sql", ") ++ sql")"
  }

  private def excludeMapping(types: Set[TopologyMappingX.Code]): SQLActionBuilderChain = {
    if (types.isEmpty) sql""
    else
      sql" AND transaction_type NOT IN (" ++ types.toSeq
        .map(t => sql"$t")
        .intercalate(sql", ") ++ sql")"
  }

  private def findAsOfExclusive(
      effective: EffectiveTime,
      subQuery: SQLActionBuilder,
      operation: String,
  )(implicit traceContext: TraceContext): Future[Seq[GenericSignedTopologyTransactionX]] = {
    queryForTransactions(asOfQuery(effective.value, asOfInclusive = false) ++ subQuery, operation)
      .map(_.result.map(_.transaction))
  }

  private def findStoredSql(
      asOfExclusive: CantonTimestamp,
      transaction: GenericTopologyTransactionX,
      subQuery: SQLActionBuilder = sql"",
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = {
    val mapping = transaction.mapping
    queryForTransactions(
      // Query for leading fields of `idx_common_topology_transactions` to enable use of this index
      sql" AND transaction_type = ${mapping.code} AND namespace = ${mapping.namespace} AND identifier = ${mapping.maybeUid
          .fold(String185.empty)(_.id.toLengthLimitedString)}"
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
  ): Future[GenericStoredTopologyTransactionsX] = {
    val query =
      sql"SELECT instance, sequenced, valid_from, valid_until FROM common_topology_transactions WHERE store_id = $transactionStoreIdName" ++
        subQuery ++ (if (!includeRejected) sql" AND rejection_reason IS NULL"
                     else sql"") ++ sql" #${orderBy} #${limit}"

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
        s"$functionFullName-$operation",
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

  override def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = {
    val query =
      sql"SELECT watermark_ts FROM common_topology_dispatching WHERE store_id =$transactionStoreIdName"
        .as[CantonTimestamp]
        .headOption
    storage.query(query, functionFullName)

  }

  override def updateDispatchingWatermark(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into common_topology_dispatching (store_id, watermark_ts)
                    VALUES ($transactionStoreIdName, $timestamp)
                 on conflict (store_id) do update
                  set
                    watermark_ts = $timestamp
                 """
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Oracle =>
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

  @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
  private def getIdFilter(
      idFilter: Option[String]
  ): SQLActionBuilderChain =
    idFilter match {
      case Some(value) if value.nonEmpty => sql" AND identifier like ${value + "%"}"
      case _ => sql""
    }

  @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
  private def getNamespaceFilter(namespaceFilter: Option[String]): SQLActionBuilderChain =
    namespaceFilter match {
      case Some(value) if value.nonEmpty => sql" AND namespace LIKE ${value + "%"}"
      case _ => sql""
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
