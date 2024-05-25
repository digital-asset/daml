// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  ApiLoggingConfig,
  CachingConfigs,
  DefaultProcessingTimeouts,
  ProcessingTimeout,
}
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.BlockSequencerStateManager.ChunkState
import com.digitalasset.canton.domain.block.data.memory.InMemorySequencerBlockStore
import com.digitalasset.canton.domain.block.data.{BlockEphemeralState, BlockInfo, EphemeralState}
import com.digitalasset.canton.domain.block.update.{
  BlockUpdate,
  BlockUpdateGenerator,
  OrderedBlockUpdate,
  SignedChunkEvents,
}
import com.digitalasset.canton.domain.block.{
  BlockEvents,
  BlockSequencerStateManager,
  BlockSequencerStateManagerBase,
  RawLedgerBlock,
  SequencerDriverHealthStatus,
}
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.SignedOrderingRequest
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerIntegration
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.domain.sequencing.traffic.RateLimitManagerTesting
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.InMemoryTrafficPurchasedStore
import com.digitalasset.canton.lifecycle.AsyncOrSyncCloseable
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.CantonPrettyPrinter
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  SendAsyncError,
  SignedContent,
}
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.StoreBasedDomainTopologyClient
import com.digitalasset.canton.topology.processing.{
  ApproximateTime,
  EffectiveTime,
  SequencedTime,
  TopologyTransactionTestFactory,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
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

    private val domainId = topologyTransactionFactory.domainId1
    private val sequencer1 = topologyTransactionFactory.sequencer1
    private val topologyStore =
      new InMemoryTopologyStore(DomainStore(domainId), loggerFactory, timeouts)

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
      .futureValue

    private val topologyClient = new StoreBasedDomainTopologyClient(
      mock[Clock],
      domainId,
      testedProtocolVersion,
      topologyStore,
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      DefaultProcessingTimeouts.testing,
      FutureSupervisor.Noop,
      loggerFactory,
    )
    topologyClient.updateHead(
      EffectiveTime(CantonTimestamp.Epoch),
      ApproximateTime(CantonTimestamp.Epoch),
      potentialTopologyChange = true,
    )
    private val cryptoApi = new DomainSyncCryptoClient(
      member = sequencer1,
      domainId,
      topologyClient,
      // This works even though the crypto owner is the domain manager!!!
      topologyTransactionFactory.cryptoApi.crypto,
      CachingConfigs.testing,
      DefaultProcessingTimeouts.testing,
      FutureSupervisor.Noop,
      loggerFactory,
    )

    private val store =
      new InMemorySequencerBlockStore(None, loggerFactory)

    private val balanceStore = new InMemoryTrafficPurchasedStore(loggerFactory)

    val fakeBlockOrderer = new FakeBlockOrderer(N)
    private val fakeBlockSequencerStateManager = new FakeBlockSequencerStateManager
    private val storage = new MemoryStorage(loggerFactory, timeouts)
    private val blockSequencer =
      new BlockSequencer(
        fakeBlockOrderer,
        name = "test",
        domainId,
        cryptoApi,
        sequencerId = sequencer1,
        fakeBlockSequencerStateManager,
        store,
        balanceStore,
        storage,
        FutureSupervisor.Noop,
        health = None,
        new SimClock(loggerFactory = loggerFactory),
        testedProtocolVersion,
        blockRateLimitManager = defaultRateLimiter,
        OrderingTimeFixMode.MakeStrictlyIncreasing,
        processingTimeouts = BlockSequencerTest.this.timeouts,
        logEventDetails = true,
        prettyPrinter = new CantonPrettyPrinter(
          ApiLoggingConfig.defaultMaxStringLength,
          ApiLoggingConfig.defaultMaxMessageLines,
        ),
        SequencerMetrics.noop(this.getClass.getName),
        loggerFactory,
        unifiedSequencer = testedUseUnifiedSequencer,
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
    override def send(signedSubmission: SignedOrderingRequest)(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncError, Unit] = ???
    override def health(implicit traceContext: TraceContext): Future[SequencerDriverHealthStatus] =
      ???
    override def acknowledge(signedAcknowledgeRequest: SignedContent[AcknowledgeRequest])(implicit
        traceContext: TraceContext
    ): Future[Unit] = ???

    override def firstBlockHeight: Long = ???

    override def orderingTimeFixMode: OrderingTimeFixMode = ???
  }

  class FakeBlockSequencerStateManager extends BlockSequencerStateManagerBase {

    override val maybeLowerTopologyTimestampBound: Option[CantonTimestamp] = None

    override def processBlock(
        bug: BlockUpdateGenerator
    ): Flow[BlockEvents, Traced[OrderedBlockUpdate[SignedChunkEvents]], NotUsed] =
      Flow[BlockEvents].mapConcat(_ => Seq.empty)

    override def applyBlockUpdate(
        dbSequencerIntegration: SequencerIntegration
    ): Flow[Traced[BlockUpdate[SignedChunkEvents]], Traced[CantonTimestamp], NotUsed] =
      Flow[Traced[BlockUpdate[SignedChunkEvents]]].map(_.map(_ => CantonTimestamp.MinValue))

    override def getHeadState: BlockSequencerStateManager.HeadState =
      BlockSequencerStateManager.HeadState(
        BlockInfo.initial,
        ChunkState.initial(BlockEphemeralState(BlockInfo.initial, EphemeralState.empty)),
      )

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq()
    override protected def timeouts: ProcessingTimeout = BlockSequencerTest.this.timeouts
    override protected def logger: TracedLogger = BlockSequencerTest.this.logger

    // No need to implement these methods for the test
    override def isMemberRegistered(member: Member): Boolean = ???
    override def readEventsForMember(member: Member, startingAt: SequencerCounter)(implicit
        traceContext: TraceContext
    ): CreateSubscription = ???
    override private[domain] def firstSequencerCounterServableForSequencer
        : com.digitalasset.canton.SequencerCounter = ???
    override def isMemberEnabled(member: com.digitalasset.canton.topology.Member): Boolean = ???
    override def waitForAcknowledgementToComplete(
        member: com.digitalasset.canton.topology.Member,
        timestamp: com.digitalasset.canton.data.CantonTimestamp,
    )(implicit
        traceContext: com.digitalasset.canton.tracing.TraceContext
    ): scala.concurrent.Future[Unit] = ???
    override def waitForMemberToBeDisabled(
        member: com.digitalasset.canton.topology.Member
    ): scala.concurrent.Future[Unit] = ???
    override def waitForPruningToComplete(
        timestamp: com.digitalasset.canton.data.CantonTimestamp
    ): (Boolean, Future[Unit]) = ???
  }
}
