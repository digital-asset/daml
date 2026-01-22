// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.String185
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalInstanceReference,
  LocalParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.crypto.{Crypto, SynchronizerCrypto}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.{Storage, StorageSingleFactory}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.synchronizer.sequencer.SequencerSnapshot
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId as AdminTopologyStoreId
import com.digitalasset.canton.topology.cache.TopologyStateWriteThroughCache
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{TimeQuery, TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.checks.{
  OptionalTopologyMappingChecks,
  RequiredTopologyMappingChecks,
  TopologyMappingChecks,
}
import com.digitalasset.canton.topology.{SynchronizerId, TopologyStateProcessor}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.digitalasset.canton.{BaseTest, FutureHelpers}

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

class TopologyStateVerification(
    referenceTime: CantonTimestamp,
    futureSupervisor: FutureSupervisor,
    clock: Clock,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
) extends HasCloseContext
    with FlagCloseable
    with NamedLogging
    with FutureHelpers {

  /** Verifies the topology history is the same on all nodes. This checks that the nodes not only
    * arrived at the same state in the end, but also that all intermediate steps where the same as
    * well
    */
  def ensureConsistentTopologyState(
      nodesToVerify: Seq[InstanceReference],
      synchronizerId: SynchronizerId,
  ): Unit = {
    val topologyHistories = nodesToVerify
      .map(node =>
        node.name -> fetchTopologyHistory(
          node,
          referenceTime,
          synchronizerId,
        )
      )

    val divergingTopologyState = topologyHistories
      .sliding(2)
      .exists {
        case Seq(
              (_, (node1Authorized, node1LatestProposals)),
              (_, (node2Authorized, node2LatestProposals)),
            ) =>
          node1Authorized.result != node2Authorized.result ||
          node1LatestProposals.result != node2LatestProposals.result
        case _otherwise => false
      }

    if (divergingTopologyState) {
      reportDivergingTopologyState(topologyHistories)
    }
  }

  /** Verifies that the sequencer snapshot is the same on all sequencers
    */
  def ensureConsistentSequencerSnapshots(
      sequencers: Seq[SequencerReference]
  ): Unit =
    if (sequencers.sizeCompare(2) >= 0) { // do nothing
      sequencers.map(n => n -> n.setup.snapshot(referenceTime)).sliding(2).foreach {
        case Seq((node1, snapshot1), (node2, snapshot2)) =>
          val snapshot1_ = withoutLastAcknowledgedAndLowerBound(snapshot1)
          val snapshot2_ = withoutLastAcknowledgedAndLowerBound(snapshot2)

          require(
            snapshot1_.hasSameContentsAs(snapshot2_),
            s"""sequencer snapshots were not the same
             |reference node ${node1.name}: $snapshot1_
             |node to verify ${node2.name}: $snapshot2_""".stripMargin,
          )
        case _otherwise => sys.error("should not happen")
      }
    }

  private def withoutLastAcknowledgedAndLowerBound(
      snapshot: SequencerSnapshot
  ): SequencerSnapshot =
    snapshot
      .copy(
        status = snapshot.status
          .copy(
            lowerBound = CantonTimestamp.MinValue,
            members = snapshot.status.members.map(
              _.copy(lastAcknowledged = None)
            ),
          )
      )(snapshot.representativeProtocolVersion)

  /** Verifies that transactions can be replayed
    */
  def ensureTopologyHistoryCanBeReplayed(
      referenceNode: InstanceReference,
      unusedNode: LocalParticipantReference,
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = TraceContext.withNewTraceContext("test") { implicit traceContext =>
    import env.*
    val (authorizedHistory, latestProposals) =
      fetchTopologyHistory(referenceNode, referenceTime, synchronizerId)

    val replayF = for {
      topologyComponents <- createTopologyComponents(
        unusedNode,
        referenceTime,
        staticSynchronizerParameters,
      )
      (topologyProcessor, topologyStore, storage) = topologyComponents
      _ = env.environment.addUserCloseable(storage)

      fullHistory = authorizedHistory.result ++ latestProposals.result

      _ <- replayTransactions(
        topologyProcessor,
        fullHistory,
      )
      replayedState <- topologyStore.inspect(
        proposals = false,
        timeQuery = TimeQuery.Range(from = None, until = None),
        asOfExclusiveO = None,
        op = None,
        types = Nil,
        idFilter = None,
        namespaceFilter = None,
      )
    } yield {

      if (fullHistory != replayedState.result.map(_.transaction)) {}
    }

    Await.result(replayF, 10.minutes).discard
  }

  private def replayTransactions(
      topologyProcessor: TopologyStateProcessor,
      transactions: Seq[GenericStoredTopologyTransaction],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Unit] =
    MonadUtil
      .sequentialTraverse(
        transactions
          .groupBy1(_.sequenced)
          .toSeq
          .sortBy { case (sequencedTime, _) => sequencedTime }
      ) { case (_, transactionsWithSameSequencedTime) =>
        topologyProcessor
          .validateAndApplyAuthorization(
            transactionsWithSameSequencedTime.head1.sequenced,
            transactionsWithSameSequencedTime.head1.validFrom,
            transactionsWithSameSequencedTime.map(_.transaction),
            expectFullAuthorization = false,
            relaxChecksForBackwardsCompatibility = false,
          )
          .flatMap { case (_, asyncResult) => asyncResult.unwrap }
      }
      .void

  private def createTopologyComponents(
      node: LocalInstanceReference,
      referenceTime: CantonTimestamp,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  )(implicit
      traceContext: TraceContext,
      env: TestConsoleEnvironment,
  ): FutureUnlessShutdown[
    (TopologyStateProcessor, TopologyStore[TopologyStoreId], Storage)
  ] = {
    import env.*

    // initialize the database schema
    node.db.migrate()

    val StorageConfig = node.config.storage match {
      case cfg: StorageConfig => cfg
      case otherwise => sys.error(s"expected an StorageConfig, but found $otherwise")
    }

    val storage =
      new StorageSingleFactory(StorageConfig)
        .create(
          connectionPoolForParticipant = false,
          None,
          environment.clock,
          Some(environment.scheduler),
          CommonMockMetrics.dbStorage,
          timeouts,
          loggerFactory,
        )
        .value
        .onShutdown(Left("unexpected shutdown"))
        .fold(err => sys.error(err), identity)

    val replayName = s"replay-verification-${referenceTime.underlying.toInstant.getEpochSecond}"
    val replayLoggerFactory = loggerFactory.append("replay", replayName)

    val indexedStringStore = IndexedStringStore.create(
      storage,
      node.config.parameters.caching.indexedStrings,
      environmentTimeouts,
      loggerFactory,
    )

    val topologyStore = TopologyStore
      .create(
        // use a temporary store to be absolutely sure that no interference with any other regular topology stores may happen
        TopologyStoreId.TemporaryStore(String185.tryCreate(replayName)),
        storage,
        indexedStringStore,
        staticSynchronizerParameters.protocolVersion,
        timeouts,
        BatchingConfig(),
        replayLoggerFactory,
      )
      .futureValueUS

    for {
      crypto <-
        Crypto
          .create(
            node.config.crypto,
            node.config.parameters.caching.kmsMetadataCache,
            node.config.parameters.caching.sessionEncryptionKeyCache,
            node.config.parameters.caching.publicKeyConversionCache,
            storage,
            Option.empty[ReplicaManager],
            ReleaseProtocolVersion(BaseTest.testedProtocolVersion),
            futureSupervisor,
            clock,
            executionContext,
            timeouts,
            BatchingConfig(),
            replayLoggerFactory,
            NoReportingTracerProvider,
          )
          .map(SynchronizerCrypto(_, staticSynchronizerParameters))
          .valueOrF(err =>
            FutureUnlessShutdown.failed(new RuntimeException(s"Failed to create crypto: $err"))
          )

      processor = TopologyStateProcessor.forTransactionProcessing(
        topologyStore,
        new TopologyStateWriteThroughCache(
          topologyStore,
          BatchAggregatorConfig(), // use default batch aggregator config
          maxCacheSize = node.config.topology.maxTopologyStateCacheItems,
          enableConsistencyChecks = node.config.topology.enableTopologyStateCacheConsistencyChecks,
          timeouts,
          loggerFactory,
        ),
        lookup =>
          new TopologyMappingChecks.All(
            RequiredTopologyMappingChecks(
              Some(crypto.staticSynchronizerParameters),
              lookup,
              replayLoggerFactory,
            ),
            new OptionalTopologyMappingChecks(topologyStore, replayLoggerFactory),
          ),
        crypto.pureCrypto,
        replayLoggerFactory,
      )
    } yield {
      (processor, topologyStore, storage)
    }
  }

  private def fetchTopologyHistory(
      node: InstanceReference,
      referenceTime: CantonTimestamp,
      store: AdminTopologyStoreId,
  ): (GenericStoredTopologyTransactions, GenericStoredTopologyTransactions) = {
    val fullyAuthorized = node.topology.transactions.list(
      store = store,
      proposals = false,
      timeQuery = TimeQuery.Range(from = None, until = Some(referenceTime)),
    )
    val latestProposals = node.topology.transactions.list(
      store = store,
      proposals = true,
      timeQuery = TimeQuery.Snapshot(referenceTime),
    )

    val latestAuthorized = fullyAuthorized.result
      .groupBy(tx => tx.mapping.uniqueKey)
      .view
      .mapValues(_.maxBy(_.sequenced))
      .toMap

    // validate for consistency between proposals and latest authorized transactions
    latestProposals.result.foreach { prop =>
      if (prop.transaction.transaction.serial.value > 1) {
        val latest = latestAuthorized.getOrElse(
          prop.mapping.uniqueKey,
          sys.error(s"did not find fully authorized transaction for proposal $prop"),
        )
        require(
          latest.transaction.transaction.serial.increment == prop.transaction.transaction.serial
        )
      }
    }

    (fullyAuthorized, latestProposals)
  }

  private def reportDivergingTopologyState(
      topologyHistories: Seq[
        (String, (GenericStoredTopologyTransactions, GenericStoredTopologyTransactions))
      ]
  ): Unit = {
    val debugStrings = topologyHistories
      .map { case (name, (authorizedHistory, latestProposals)) =>
        s"""Node: $name
           |------------ Authorized History
           |${authorizedHistory.result.mkString("\n")}
           |------------ Latest Proposals
           |${latestProposals.result.mkString("\n")}""".stripMargin
      }
      .intercalate("============")
    logger.error(s"Detected diverging topology state!\n$debugStrings")(
      TraceContext.empty
    )
  }

}
