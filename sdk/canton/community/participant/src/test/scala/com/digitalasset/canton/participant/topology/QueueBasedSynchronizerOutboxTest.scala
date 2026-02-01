// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.implicits.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.sequencer.RegisterTopologyTransactionHandle
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{
  BatchAggregatorConfig,
  NonNegativeFiniteDuration,
  ProcessingTimeout,
  TopologyConfig,
}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{SigningKeyUsage, SynchronizerCrypto}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule, TracedLogger}
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast.State
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  StoreBasedSynchronizerTopologyClient,
  SynchronizerTopologyClientWithInit,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  GenericTopologyTransaction,
  TxHash,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, MonadUtil}
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  ProtocolVersionChecksAsyncWordSpec,
  SequencerCounter,
  SynchronizerAlias,
}
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.annotation.nowarn
import scala.jdk.CollectionConverters.*
import scala.util.chaining.scalaUtilChainingOps

class QueueBasedSynchronizerOutboxTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with HasExecutionContext
    with FailOnShutdown {
  import DefaultTestIdentities.*

  private lazy val topologyConfig = TopologyConfig.forTesting.copy(
    topologyTransactionObservationTimeout = NonNegativeFiniteDuration.ofSeconds(2),
    broadcastRetryDelay = NonNegativeFiniteDuration.ofSeconds(1),
  )
  private lazy val clock = new WallClock(timeouts, loggerFactory)
  private lazy val crypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)
  private lazy val synchronizerCrypto =
    SynchronizerCrypto(crypto, defaultStaticSynchronizerParameters)
  private lazy val publicKey =
    crypto.generateSymbolicSigningKey(usage = SigningKeyUsage.NamespaceOnly)
  private lazy val namespace = Namespace(publicKey.id)
  private lazy val synchronizer = SynchronizerAlias.tryCreate("target")
  private def mkPTP(name: String) = PartyToParticipant.tryCreate(
    PartyId(UniqueIdentifier.tryCreate(name, namespace)),
    PositiveInt.one,
    Seq.empty,
  )
  private lazy val transactions =
    Seq("alpha", "beta", "gamma", "delta").map(mkPTP).map(txAddFromMapping)
  private lazy val slice1 = transactions.slice(0, 2)
  private lazy val slice2 = transactions.slice(slice1.length, transactions.length)

  private val rootCertF = SignedTopologyTransaction
    .signAndCreate(
      TopologyTransaction(
        op = TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        NamespaceDelegation.tryCreate(namespace, publicKey, CanSignAllMappings),
        testedProtocolVersion,
      ),
      signingKeys = NonEmpty(
        Set,
        publicKey.fingerprint,
      ),
      isProposal = false,
      crypto.privateCrypto,
      testedProtocolVersion,
    )
    .value
    .map(_.valueOrFail("error creating root certificate"))

  private def mk(
      expect: Int,
      responses: Iterator[TopologyTransactionsBroadcast.State] =
        Iterator.continually(TopologyTransactionsBroadcast.State.Accepted),
      rejections: Iterator[Option[TopologyTransactionRejection]] = Iterator.continually(None),
      dropSequencedBroadcast: Iterator[Boolean] = Iterator.continually(false),
      autoFlush: Boolean = true,
  ): FutureUnlessShutdown[
    (
        InMemoryTopologyStore[TopologyStoreId.SynchronizerStore],
        SynchronizerTopologyManager,
        MockHandle,
        SynchronizerTopologyClientWithInit,
    )
  ] = {
    val target = new InMemoryTopologyStore(
      TopologyStoreId.SynchronizerStore(DefaultTestIdentities.physicalSynchronizerId),
      testedProtocolVersion,
      loggerFactory,
      timeouts,
    )
    val queue = new SynchronizerOutboxQueue(loggerFactory)
    val manager = new SynchronizerTopologyManager(
      participant1.uid,
      clock,
      synchronizerCrypto,
      defaultStaticSynchronizerParameters,
      BatchAggregatorConfig.defaultsForTesting,
      topologyConfig,
      target,
      queue,
      dispatchQueueBackpressureLimit = NonNegativeInt.tryCreate(10),
      disableOptionalTopologyChecks = false,
      // we don't need the validation logic to run, because we control the outcome of transactions manually
      exitOnFatalFailures = true,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    val client = new StoreBasedSynchronizerTopologyClient(
      clock,
      defaultStaticSynchronizerParameters,
      store = target,
      packageDependencyResolver = NoPackageDependencies,
      topologyConfig = TopologyConfig(),
      timeouts = timeouts,
      futureSupervisor = futureSupervisor,
      loggerFactory = loggerFactory,
    )
    val handle =
      new MockHandle(
        expect,
        responses = responses,
        store = target,
        targetClient = client,
        rejections = rejections,
        dropSequencedBroadcast = dropSequencedBroadcast,
        autoFlush = autoFlush,
      )

    for {
      // in this test (as opposed to StoreBasedSynchronizerOutboxTest) we need to
      // always have the root certificate in the topology store, otherwise the
      // IDDs won't pass validation.
      rootCert <- rootCertF
      _ <-
        target
          .update(
            sequenced = SequencedTime.MinValue,
            effective = EffectiveTime.MinValue,
            removals = Map.empty,
            additions = Seq(ValidatedTopologyTransaction(rootCert)),
          )
    } yield (target, manager, handle, client)
  }

  private class MockHandle(
      expectI: Int,
      responses: Iterator[State],
      store: TopologyStore[TopologyStoreId],
      targetClient: SynchronizerTopologyClientWithInit,
      rejections: Iterator[Option[TopologyTransactionRejection]] = Iterator.continually(None),
      dropSequencedBroadcast: Iterator[Boolean] = Iterator.continually(false),
      autoFlush: Boolean = true,
  ) extends RegisterTopologyTransactionHandle {
    val transactionsBuffer: CopyOnWriteArrayList[GenericSignedTopologyTransaction] =
      new CopyOnWriteArrayList()
    val batches: CopyOnWriteArrayList[Seq[GenericSignedTopologyTransaction]] =
      new CopyOnWriteArrayList()
    private val allTransactionsObservedPromise = new AtomicReference(
      PromiseUnlessShutdown.supervised[Seq[Seq[GenericSignedTopologyTransaction]]](
        "promise",
        futureSupervisor,
      )
    )
    val preFlushNotification = new AtomicReference(PromiseUnlessShutdown.unsupervised[Unit]())
    val flushBlocker = new AtomicReference(PromiseUnlessShutdown.unsupervised[Unit]())
    val topologyClientConnected = new AtomicBoolean(true)

    private val expect = new AtomicInteger(expectI)

    override def submit(
        transactions: Seq[GenericSignedTopologyTransaction]
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Seq[TopologyTransactionsBroadcast.State]] = {
      logger.debug(s"Submitting ${transactions.length} transactions")

      val dropBroadcast = dropSequencedBroadcast.next()

      if (!dropBroadcast) {
        transactionsBuffer.addAll(transactions.asJava)
        batches.add(transactions)
      }
      val finalResult = transactions.map(_ => responses.next())
      val localProcessingF = for {
        _ <- MonadUtil.sequentialTraverse(transactions) { x =>
          if (dropBroadcast) logger.debug(s"Dropping $x")
          else logger.debug(s"Processing $x")
          if (autoFlush) flushBlocker.get().outcome_(())
          val ts = CantonTimestamp.now()
          if (finalResult.forall(_ == State.Accepted) && !dropBroadcast) {
            preFlushNotification.getAndSet(PromiseUnlessShutdown.unsupervised()).outcome_(())
            for {
              _ <- flushBlocker.get().futureUS
              _ = flushBlocker.set(PromiseUnlessShutdown.unsupervised())
              _ <- store
                .update(
                  SequencedTime(ts),
                  EffectiveTime(ts),
                  additions = List(ValidatedTopologyTransaction(x, rejections.next())),
                  // dumbed down version of how to "append" ValidatedTopologyTransactions:
                  removals = Option
                    .when(x.operation == TopologyChangeOp.Remove)(
                      x.mapping.uniqueKey -> (x.serial.some, Set.empty[TxHash])
                    )
                    .toList
                    .toMap,
                )
              _ <- MonadUtil.when(topologyClientConnected.get())(
                targetClient
                  .observed(
                    SequencedTime(ts),
                    EffectiveTime(ts),
                    SequencerCounter(3),
                    if (rejections.isEmpty) Seq(x) else Seq.empty,
                  )
              )

            } yield ()
          } else FutureUnlessShutdown.unit
        }
        _ = if (transactionsBuffer.size() >= expect.get()) {
          allTransactionsObservedPromise
            .get()
            .success(UnlessShutdown.Outcome(batches.asScala.toSeq))
        }
      } yield {
        logger.debug(s"Done with observed ${transactions.length} transactions")
      }
      FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(localProcessingF, "local processing failed")
      FutureUnlessShutdown.pure(finalResult)
    }

    def disconnectTopologyClient(): Unit = topologyClientConnected.set(false)

    def buffer: Seq[GenericSignedTopologyTransaction] = transactionsBuffer.asScala.toSeq

    def clear(expectI: Int): Seq[GenericSignedTopologyTransaction] = {
      val ret = buffer
      transactionsBuffer.clear()
      expect.set(expectI)
      allTransactionsObservedPromise.set(PromiseUnlessShutdown.unsupervised())
      ret
    }

    def allObserved(): FutureUnlessShutdown[Unit] =
      allTransactionsObservedPromise.get().futureUS.void

    override protected def timeouts: ProcessingTimeout = ProcessingTimeout()
    override protected def logger: TracedLogger = QueueBasedSynchronizerOutboxTest.this.logger
  }

  private def push(
      manager: SynchronizerTopologyManager,
      transactions: Seq[GenericTopologyTransaction],
      waitToBecomeEffective: Option[NonNegativeFiniteDuration] = None,
  ): FutureUnlessShutdown[
    Either[TopologyManagerError, Seq[GenericSignedTopologyTransaction]]
  ] =
    MonadUtil
      .sequentialTraverse(transactions)(tx =>
        manager.proposeAndAuthorize(
          tx.operation,
          tx.mapping,
          tx.serial.some,
          signingKeys = Seq(publicKey.fingerprint),
          testedProtocolVersion,
          expectFullAuthorization = true,
          waitToBecomeEffective = waitToBecomeEffective,
        )
      )
      .value

  private def outboxConnected(
      manager: SynchronizerTopologyManager,
      handle: RegisterTopologyTransactionHandle,
      client: SynchronizerTopologyClientWithInit,
      target: TopologyStore[TopologyStoreId.SynchronizerStore],
      topologyConfig: TopologyConfig = topologyConfig,
  ): FutureUnlessShutdown[QueueBasedSynchronizerOutbox] = {
    val synchronizerOutbox = new QueueBasedSynchronizerOutbox(
      synchronizer,
      participant1,
      handle,
      client,
      manager.outboxQueue,
      target,
      timeouts,
      loggerFactory,
      synchronizerCrypto,
      topologyConfig,
    )
    synchronizerOutbox
      .startup()
      .fold[QueueBasedSynchronizerOutbox](
        s => fail(s"Failed to start synchronizer outbox $s"),
        _ =>
          synchronizerOutbox.tap(outbox =>
            // add the outbox as an observer since these unit tests avoid instantiating the ParticipantTopologyDispatcher
            manager.addObserver(new TopologyManagerObserver {
              override def addedNewTransactions(
                  timestamp: CantonTimestamp,
                  transactions: Seq[GenericSignedTopologyTransaction],
              )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
                val num = transactions.size
                outbox.newTransactionsAdded(timestamp, num)
              }
            })
          ),
      )
  }

  private def disconnectOutboxWhenIdle(
      manager: SynchronizerTopologyManager,
      outbox: QueueBasedSynchronizerOutbox,
  ): Unit = {
    manager.clearObservers()
    // Wait until all messages have been dispatched, only then disconnect
    eventually()(outbox.queueSize shouldBe 0)
    outbox.close()
  }

  private def txAddFromMapping(mapping: TopologyMapping) =
    TopologyTransaction(
      TopologyChangeOp.Replace,
      serial = PositiveInt.one,
      mapping,
      testedProtocolVersion,
    )

  private def headTransactions(store: TopologyStore[?]) = store
    .findPositiveTransactions(
      asOf = CantonTimestamp.MaxValue,
      asOfInclusive = false,
      isProposal = false,
      types = TopologyMapping.Code.all,
      filterUid = None,
      filterNamespace = None,
    )
    .map(x => StoredTopologyTransactions(x.result.filter(_.validUntil.isEmpty)))

  "dispatcher" should {

    "dispatch transaction on new connect" in {
      for {
        (target, manager, handle, client) <- mk(transactions.length)
        res <- push(manager, transactions)
        _ <- outboxConnected(manager, handle, client, target)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[?]]
        handle.buffer should have length transactions.length.toLong
      }
    }

    "dispatch transaction on existing connections" in {
      for {
        (target, manager, handle, client) <- mk(transactions.length)
        _ <- outboxConnected(manager, handle, client, target)
        res <- push(manager, transactions)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[?]]
        handle.buffer should have length transactions.length.toLong
      }
    }

    "dispatch transactions continuously respecting the batch size" in {
      for {
        (target, manager, handle, client) <- mk(slice1.length)
        _res <- push(manager, slice1)
        _ <- outboxConnected(
          manager,
          handle,
          client,
          target,
          topologyConfig.copy(broadcastBatchSize = PositiveInt.one),
        )
        _ <- handle.allObserved()
        observed1 = handle.clear(slice2.length)
        _ <- push(manager, slice2)
        _ <- handle.allObserved()
      } yield {
        observed1.map(_.transaction) shouldBe slice1
        handle.buffer.map(_.transaction) shouldBe slice2
        handle.batches should not be empty
        forAll(handle.batches)(_.size shouldBe 1)
      }
    }

    "not dispatch old data when reconnected" in {
      for {
        (target, manager, handle, client) <- mk(slice1.length)
        outbox <- outboxConnected(manager, handle, client, target)
        _ <- push(manager, slice1)
        _ <- handle.allObserved()
        _ = disconnectOutboxWhenIdle(manager, outbox)
        _ = handle.clear(slice2.length)
        res2 <- push(manager, slice2)
        _ <- outboxConnected(manager, handle, client, target)
        _ <- handle.allObserved()
      } yield {
        res2.value shouldBe a[Seq[?]]
        handle.buffer.map(_.transaction) shouldBe slice2
      }
    }

    "correctly find a remove in source store" in {
      val midRevert = transactions(1).reverse
      val another = txAddFromMapping(mkPTP("eta"))

      for {
        (target, manager, handle, client) <- mk(transactions.length)
        outbox <- outboxConnected(manager, handle, client, target)
        _ <- push(manager, transactions)
        _ <- handle.allObserved()
        _ = disconnectOutboxWhenIdle(manager, outbox)
        // add a remove and another add
        _ <- push(manager, Seq(midRevert, another))
        // and ensure both are not in the new store
        tis <- headTransactions(target).map(_.toTopologyState)
        _ = tis should contain(midRevert.mapping)
        _ = tis should not contain another.mapping
        // re-connect
        _ = handle.clear(2)
        _ <- outboxConnected(manager, handle, client, target)
        _ <- handle.allObserved()
        tis <- headTransactions(target).map(_.toTopologyState)
      } yield {
        tis should not contain midRevert.mapping
        tis should contain(another.mapping)
      }
    }

    "handle rejected transactions" in {
      for {
        (target, manager, handle, client) <-
          mk(
            transactions.size,
            rejections = Iterator.continually(
              Some(TopologyTransactionRejection.Authorization.NotAuthorizedByNamespaceKey)
            ),
          )
        _ <- outboxConnected(manager, handle, client, target)
        res <- push(manager, transactions)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[?]]
        handle.buffer should have length transactions.length.toLong
      }
    }

    "handle failed transactions" in {
      @nowarn val Seq(tx1) = transactions.take(1)
      @nowarn val Seq(tx2) = transactions.slice(1, 2)

      lazy val action = for {
        (target, manager, handle, client) <-
          mk(
            2,
            responses = Iterator(
              // we fail the transaction on the first attempt
              State.Failed,
              // When it gets submitted again, let's have it be successful
              State.Accepted,
              State.Accepted,
            ),
          )
        _ <- outboxConnected(manager, handle, client, target)
        res1 <- push(manager, Seq(tx1))
        res2 <- push(manager, Seq(tx2))
        _ <- handle.allObserved()

      } yield {
        res1.value shouldBe a[Seq[?]]
        res2.value shouldBe a[Seq[?]]
        handle.buffer should have length 3
      }
      loggerFactory.assertLogs(
        action,
        _.warningMessage should include("failed the following topology transactions"),
      )
    }

    "handle dropped transactions" in {
      for {
        (target, manager, handle, client) <- mk(
          1,
          // drop the first submission, but not the second
          dropSequencedBroadcast = Iterator(true, false),
        )
        _ <- outboxConnected(manager, handle, client, target)
        res1 <- loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
          push(
            manager,
            transactions.take(1),
            // take into account the timeouts lowered in this test, and be generous in the timeout for getting a response:
            // * submission timeout (2s)
            // * retryDelay (1s)
            Some(NonNegativeFiniteDuration.ofSeconds(8)),
          ),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include(
                  "Did not observe transactions in target synchronizer store."
                ),
                "outbox times out waiting to observe topology transaction",
              )
            )
          ),
        )
      } yield {
        res1.value.map(_.transaction) shouldBe transactions.take(1)
      }
    }

    // TODO(#30401) re-enable when fixed
    "handle being closed while transactions are pending" in {
      // This test executes the following scenario:
      // 1. submit TB1 (topology broadcast 1) and keep it from being fully flushed to the topology store.
      //    this is only needed to reconstruct the failure observed in production.
      // 2. submit TB2, which just gets put into the unsent-queue, because TB1 has not yet been fully processed.
      //    there is now 1 pending transaction and 1 unsent transaction.
      // 3. simulate disconnecting from the synchronizer, by disconnecting the topology client from the processing.
      //    in the production code, this happens by shutting down the topology processor for the synchronizer. in
      //    this test, we do it via handle.disconnectTopologyClient. Additionally, close the outbox and the topology client.
      // 4. d

      for {
        (target, manager, handle, client) <- mk(
          0,
          autoFlush = false,
        )
        outbox <- outboxConnected(
          manager,
          handle,
          client,
          target,
          // make the observation timeout long, so that we surely can shut down while
          // the transaction is still pending
          topologyConfig.copy(topologyTransactionObservationTimeout =
            NonNegativeFiniteDuration.ofMinutes(2)
          ),
        )
        preFlushF = handle.preFlushNotification.get().futureUS

        _ = push(
          manager,
          transactions.take(1),
          None,
        )

        // wait until we get just before flushing the topology cache to the store, so that we can push
        // another transaction that will end up in the unsent-queue, while the first one is still being processed.
        _ <- preFlushF

        // push another transaction, so that we have one pending and one unsent transaction
        _ = push(
          manager,
          transactions.slice(1, 2),
          None,
        )

        _ = eventually() {
          manager.outboxQueue.numInProcessTransactions shouldBe 1
          manager.outboxQueue.numUnsentTransactions shouldBe 1
        }

        // disconnect the topology client from the processing pipeline to simulate a disconnection from the synchronizer
        // at exactly that moment.
        _ = handle.disconnectTopologyClient()

        // shut down the topology client and outbox and disconnect the outbox from the topology manager
        _ = client.close()
        _ = manager.clearObservers()
        _ = outbox.close()

        // if the shutdown procedure is properly wired up, the pending transactions gets put into the unsent queue
        _ = eventually() {
          manager.outboxQueue.numInProcessTransactions shouldBe 0
          manager.outboxQueue.numUnsentTransactions shouldBe 2
        }

        // release the flush blocker to write the transaction to the store.
        _ = handle.flushBlocker.get().outcome_(())

        // now simulate reconnecting to the synchronizer by setting up a new
        // * topology client
        // * handle
        // * outbox

        client2 = new StoreBasedSynchronizerTopologyClient(
          clock,
          defaultStaticSynchronizerParameters,
          store = target,
          packageDependencyResolver = NoPackageDependencies,
          topologyConfig = TopologyConfig(),
          timeouts = timeouts,
          futureSupervisor = futureSupervisor,
          loggerFactory = loggerFactory,
        )

        handle2 = new MockHandle(
          // even though we pushed two transactions before, the one that was pending ultimately made it into the store
          // and therefore will be filtered out.
          // the test pushes another transaction to this new outbox, to show that the state recovers properly.
          2,
          responses = Iterator.continually(TopologyTransactionsBroadcast.State.Accepted),
          store = manager.store,
          targetClient = client2,
          autoFlush = true,
        )

        // connecting the outbox is where the production error happened, because
        // the outbox tried to dequeue, while there were still pending transactions.
        // this doesn't happen anymore, because the shutdown logic now works correctly.
        _ <- outboxConnected(manager, handle2, client2, target)

        _ = push(manager, transactions.slice(2, 3))

        _ <- handle2.allObserved()

      } yield {
        eventually() {
          manager.outboxQueue.numUnsentTransactions shouldBe 0
          manager.outboxQueue.numInProcessTransactions shouldBe 0
        }
        handle2.buffer
          .map(_.transaction) should contain theSameElementsInOrderAs (transactions.slice(1, 3))

      }
    }

  }
}
