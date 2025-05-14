// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.implicits.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.sequencer.RegisterTopologyTransactionHandle
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{SigningKeyUsage, SynchronizerCrypto}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast.State
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  IdentityProvidingServiceClient,
  StoreBasedSynchronizerTopologyClient,
  SynchronizerTopologyClientWithInit,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyTransaction.GenericTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  ProtocolVersionChecksAsyncWordSpec,
  SequencerCounter,
  SynchronizerAlias,
}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.annotation.nowarn
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.chaining.scalaUtilChainingOps

class QueueBasedSynchronizerOutboxTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with HasExecutionContext
    with FailOnShutdown {
  import DefaultTestIdentities.*

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
  ): FutureUnlessShutdown[
    (
        InMemoryTopologyStore[TopologyStoreId.SynchronizerStore],
        SynchronizerTopologyManager,
        MockHandle,
        StoreBasedSynchronizerTopologyClient,
    )
  ] = {
    val target = new InMemoryTopologyStore(
      TopologyStoreId.SynchronizerStore(DefaultTestIdentities.synchronizerId),
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
      target,
      queue,
      // we don't need the validation logic to run, because we control the outcome of transactions manually
      exitOnFatalFailures = true,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    val client = new StoreBasedSynchronizerTopologyClient(
      clock,
      DefaultTestIdentities.physicalSynchronizerId,
      store = target,
      packageDependenciesResolver = StoreBasedSynchronizerTopologyClient.NoPackageDependencies,
      ips = new IdentityProvidingServiceClient(),
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
      )

    for {
      // in the this test (as opposed to StoreBasedSynchronizerOutboxTest) we need to
      // always have the root certificate in the topology store, otherwise the
      // IDDs won't pass validation.
      rootCert <- rootCertF
      _ <-
        target
          .update(
            sequenced = SequencedTime.MinValue,
            effective = EffectiveTime.MinValue,
            removeMapping = Map.empty,
            removeTxs = Set.empty,
            additions = Seq(ValidatedTopologyTransaction(rootCert)),
          )
    } yield (target, manager, handle, client)
  }

  private class MockHandle(
      expectI: Int,
      responses: Iterator[State],
      store: TopologyStore[TopologyStoreId],
      targetClient: StoreBasedSynchronizerTopologyClient,
      rejections: Iterator[Option[TopologyTransactionRejection]] = Iterator.continually(None),
  ) extends RegisterTopologyTransactionHandle {
    val buffer: mutable.ListBuffer[GenericSignedTopologyTransaction] = ListBuffer()
    val batches: mutable.ListBuffer[Seq[GenericSignedTopologyTransaction]] = ListBuffer()
    private val promise = new AtomicReference(
      PromiseUnlessShutdown.supervised[Seq[Seq[GenericSignedTopologyTransaction]]](
        "promise",
        futureSupervisor,
      )
    )
    private val expect = new AtomicInteger(expectI)

    override def submit(
        transactions: Seq[GenericSignedTopologyTransaction]
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Seq[TopologyTransactionsBroadcast.State]] = {
      logger.debug(s"Observed ${transactions.length} transactions")
      buffer ++= transactions
      batches += transactions
      val finalResult = transactions.map(_ => responses.next())
      for {
        _ <- MonadUtil.sequentialTraverse(transactions) { x =>
          logger.debug(s"Processing $x")
          val ts = CantonTimestamp.now()
          if (finalResult.forall(_ == State.Accepted))
            store
              .update(
                SequencedTime(ts),
                EffectiveTime(ts),
                additions = List(ValidatedTopologyTransaction(x, rejections.next())),
                // dumbed down version of how to "append" ValidatedTopologyTransactions:
                removeMapping = Option
                  .when(x.operation == TopologyChangeOp.Remove)(
                    x.mapping.uniqueKey -> x.serial
                  )
                  .toList
                  .toMap,
                removeTxs = Set.empty,
              )
              .flatMap(_ =>
                targetClient
                  .observed(
                    SequencedTime(ts),
                    EffectiveTime(ts),
                    SequencerCounter(3),
                    if (rejections.isEmpty) Seq(x) else Seq.empty,
                  )
              )
          else FutureUnlessShutdown.unit
        }
        _ = if (buffer.sizeIs >= expect.get()) {
          promise.get().success(UnlessShutdown.Outcome(batches.toSeq))
        }
      } yield {
        logger.debug(s"Done with observed ${transactions.length} transactions")
        finalResult
      }
    }

    def clear(expectI: Int): Seq[GenericSignedTopologyTransaction] = {
      val ret = buffer.toList
      buffer.clear()
      expect.set(expectI)
      promise.set(PromiseUnlessShutdown.unsupervised())
      ret
    }

    def allObserved(): FutureUnlessShutdown[Unit] =
      promise.get().futureUS.void

    override protected def timeouts: ProcessingTimeout = ProcessingTimeout()
    override protected def logger: TracedLogger = QueueBasedSynchronizerOutboxTest.this.logger
  }

  private def push(
      manager: SynchronizerTopologyManager,
      transactions: Seq[GenericTopologyTransaction],
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
          waitToBecomeEffective = None,
        )
      )
      .value

  private def outboxConnected(
      manager: SynchronizerTopologyManager,
      handle: RegisterTopologyTransactionHandle,
      client: SynchronizerTopologyClientWithInit,
      target: TopologyStore[TopologyStoreId.SynchronizerStore],
      broadcastBatchSize: PositiveInt = TopologyConfig.defaultBroadcastBatchSize,
  ): FutureUnlessShutdown[QueueBasedSynchronizerOutbox] = {
    val synchronizerOutbox = new QueueBasedSynchronizerOutbox(
      synchronizer,
      DefaultTestIdentities.physicalSynchronizerId,
      participant1,
      testedProtocolVersion,
      handle,
      client,
      manager.outboxQueue,
      target,
      timeouts,
      loggerFactory,
      synchronizerCrypto,
      broadcastBatchSize,
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
        _ <- outboxConnected(manager, handle, client, target, broadcastBatchSize = PositiveInt.one)
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
            rejections = Iterator.continually(Some(TopologyTransactionRejection.NotAuthorized)),
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
  }
}
