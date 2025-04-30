// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  ApiLoggingConfig,
  BatchingConfig,
  DefaultProcessingTimeouts,
  ProcessingTimeout,
}
import com.digitalasset.canton.crypto.{SynchronizerCryptoClient, SynchronizerCryptoPureApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.CantonPrettyPrinter
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  SequencerDeliverError,
  SignedContent,
}
import com.digitalasset.canton.synchronizer.block.*
import com.digitalasset.canton.synchronizer.block.BlockSequencerStateManager.ChunkState
import com.digitalasset.canton.synchronizer.block.data.memory.InMemorySequencerBlockStore
import com.digitalasset.canton.synchronizer.block.data.{BlockEphemeralState, BlockInfo}
import com.digitalasset.canton.synchronizer.block.update.{
  BlockUpdate,
  BlockUpdateGenerator,
  OrderedBlockUpdate,
}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedOrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.store.InMemorySequencerStore
import com.digitalasset.canton.synchronizer.sequencer.{BlockSequencerConfig, SequencerIntegration}
import com.digitalasset.canton.synchronizer.sequencing.traffic.RateLimitManagerTesting
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.memory.InMemoryTrafficPurchasedStore
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.client.{
  IdentityProvidingServiceClient,
  StoreBasedSynchronizerTopologyClient,
}
import com.digitalasset.canton.topology.processing.{
  ApproximateTime,
  EffectiveTime,
  SequencedTime,
  TopologyTransactionTestFactory,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches, Materializer}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}

class BlockSequencerTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with RateLimitManagerTesting {

  "BlockSequencer" should {
    "process a lot of blocks during catch up" in withEnv { implicit env =>
      env.fakeBlockOrderer.completed.future.map(_ => succeed)
    }
  }

  private val topologyTransactionFactory =
    new TopologyTransactionTestFactory(loggerFactory, executorService)

  private val N = 1_000_000

  private def withEnv[T](test: Environment => Future[T]): Future[T] = {
    val env = new Environment
    complete {
      test(env)
    } lastly env.close()
  }

  private class Environment extends AutoCloseable {
    private val actorSystem = ActorSystem()
    implicit val materializer: Materializer = Materializer(actorSystem)

    private val synchronizerId = topologyTransactionFactory.synchronizerId1
    private val sequencer1 = topologyTransactionFactory.sequencer1
    private val topologyStore =
      new InMemoryTopologyStore(
        SynchronizerStore(synchronizerId),
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      )

    topologyStore
      .update(
        SequencedTime(CantonTimestamp.Epoch),
        EffectiveTime(CantonTimestamp.Epoch),
        removeMapping = Map.empty,
        removeTxs = Set.empty,
        additions = Seq(
          topologyTransactionFactory.ns1k1_k1,
          topologyTransactionFactory.okmS1k7_k1,
          topologyTransactionFactory.dmp1_k1,
          topologyTransactionFactory.okm1bk5k1E_k1, // this one to allow verification of the sender's signature
        ).map(ValidatedTopologyTransaction(_, rejectionReason = None)),
      )
      .futureValueUS

    private val topologyClient = new StoreBasedSynchronizerTopologyClient(
      mock[Clock],
      synchronizerId,
      topologyStore,
      StoreBasedSynchronizerTopologyClient.NoPackageDependencies,
      new IdentityProvidingServiceClient(),
      DefaultProcessingTimeouts.testing,
      FutureSupervisor.Noop,
      loggerFactory,
    )
    topologyClient.updateHead(
      SequencedTime(CantonTimestamp.Epoch),
      EffectiveTime(CantonTimestamp.Epoch),
      ApproximateTime(CantonTimestamp.Epoch),
      potentialTopologyChange = true,
    )
    private val cryptoApi = SynchronizerCryptoClient.create(
      member = sequencer1,
      synchronizerId,
      topologyClient,
      defaultStaticSynchronizerParameters,
      topologyTransactionFactory.cryptoApi.crypto,
      new SynchronizerCryptoPureApi(
        defaultStaticSynchronizerParameters,
        topologyTransactionFactory.cryptoApi.crypto.pureCrypto,
      ),
      BatchingConfig().parallelism,
      DefaultProcessingTimeouts.testing,
      FutureSupervisor.Noop,
      loggerFactory,
    )

    private val balanceStore = new InMemoryTrafficPurchasedStore(loggerFactory)

    val fakeBlockOrderer = new FakeBlockOrderer(N)
    private val fakeBlockSequencerStateManager = new FakeBlockSequencerStateManager
    private val fakeDbSequencerStore = new InMemorySequencerStore(
      testedProtocolVersion,
      sequencer1,
      blockSequencerMode = true,
      loggerFactory,
    )
    private val storage = new MemoryStorage(loggerFactory, timeouts)
    private val store =
      new InMemorySequencerBlockStore(
        new InMemorySequencerStore(
          testedProtocolVersion,
          sequencer1,
          blockSequencerMode = true,
          loggerFactory,
        ),
        loggerFactory,
      )
    private val blockSequencer =
      new BlockSequencer(
        blockOrderer = fakeBlockOrderer,
        name = "test",
        synchronizerId = synchronizerId,
        cryptoApi = cryptoApi,
        sequencerId = sequencer1,
        fakeBlockSequencerStateManager,
        store,
        dbSequencerStore = fakeDbSequencerStore,
        BlockSequencerConfig(),
        balanceStore,
        storage,
        FutureSupervisor.Noop,
        health = None,
        clock = new SimClock(loggerFactory = loggerFactory),
        protocolVersion = testedProtocolVersion,
        blockRateLimitManager = defaultRateLimiter,
        orderingTimeFixMode = OrderingTimeFixMode.MakeStrictlyIncreasing,
        processingTimeouts = BlockSequencerTest.this.timeouts,
        logEventDetails = true,
        prettyPrinter = new CantonPrettyPrinter(
          ApiLoggingConfig.defaultMaxStringLength,
          ApiLoggingConfig.defaultMaxMessageLines,
        ),
        metrics = SequencerMetrics.noop(this.getClass.getName),
        loggerFactory = loggerFactory,
        exitOnFatalFailures = true,
        runtimeReady = FutureUnlessShutdown.unit,
      )

    override def close(): Unit = {
      blockSequencer.close()
      storage.close()
      topologyClient.close()
      topologyStore.close()
      materializer.shutdown()
      Await.result(actorSystem.terminate(), 10.seconds)
    }
  }

  class FakeBlockOrderer(n: Int) extends BlockOrderer {

    val completed: Promise[Unit] = Promise()

    override def subscribe()(implicit
        traceContext: TraceContext
    ): Source[RawLedgerBlock, KillSwitch] =
      Source
        .fromIterator { () =>
          LazyList
            .from(0)
            .takeWhile(_ < n)
            .map { i =>
              if (n == i + 1)
                completed.success(())
              RawLedgerBlock(i.toLong, Seq.empty)
            }
            .iterator
        }
        .viaMat(KillSwitches.single)(Keep.right)

    override def close(): Unit = ()

    // No need to implement these methods for the test
    override def send(signedOrderingRequest: SignedOrderingRequest)(implicit
        traceContext: TraceContext
    ): EitherT[Future, SequencerDeliverError, Unit] = ???
    override def health(implicit traceContext: TraceContext): Future[SequencerDriverHealthStatus] =
      ???
    override def acknowledge(signedAcknowledgeRequest: SignedContent[AcknowledgeRequest])(implicit
        traceContext: TraceContext
    ): Future[Unit] = ???

    override def firstBlockHeight: Long = ???

    override def orderingTimeFixMode: OrderingTimeFixMode = ???

    override def sequencerSnapshotAdditionalInfo(
        timestamp: CantonTimestamp
    ): EitherT[Future, SequencerError, Option[v30.BftSequencerSnapshotAdditionalInfo]] = ???
  }

  class FakeBlockSequencerStateManager extends BlockSequencerStateManagerBase {
    override def processBlock(
        bug: BlockUpdateGenerator
    ): Flow[BlockEvents, Traced[OrderedBlockUpdate], NotUsed] =
      Flow[BlockEvents].mapConcat(_ => Seq.empty)

    override def applyBlockUpdate(
        dbSequencerIntegration: SequencerIntegration
    ): Flow[Traced[BlockUpdate], Traced[CantonTimestamp], NotUsed] =
      Flow[Traced[BlockUpdate]].map(_.map(_ => CantonTimestamp.MinValue))

    override def getHeadState: BlockSequencerStateManager.HeadState =
      BlockSequencerStateManager.HeadState(
        BlockInfo.initial,
        ChunkState.initial(BlockEphemeralState.empty),
      )

    override protected def timeouts: ProcessingTimeout = BlockSequencerTest.this.timeouts
    override protected def logger: TracedLogger = BlockSequencerTest.this.logger

    // No need to implement these methods for the test
    override def waitForAcknowledgementToComplete(
        member: com.digitalasset.canton.topology.Member,
        timestamp: com.digitalasset.canton.data.CantonTimestamp,
    )(implicit
        traceContext: com.digitalasset.canton.tracing.TraceContext
    ): scala.concurrent.Future[Unit] = ???
  }
}
