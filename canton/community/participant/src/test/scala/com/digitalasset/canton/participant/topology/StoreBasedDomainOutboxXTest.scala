// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.implicits.*
import com.digitalasset.canton.common.domain.RegisterTopologyTransactionHandleCommon
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcastX
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcastX.State
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  DomainTopologyClientWithInit,
  StoreBasedDomainTopologyClient,
  StoreBasedDomainTopologyClientX,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreX
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyChangeOpX.{Remove, Replace}
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.GenericTopologyTransactionX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{
  BaseTest,
  DomainAlias,
  ProtocolVersionChecksAsyncWordSpec,
  SequencerCounter,
  config,
}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.annotation.nowarn
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.util.chaining.scalaUtilChainingOps

class StoreBasedDomainOutboxXTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec {
  import DefaultTestIdentities.*

  private val clock = new WallClock(timeouts, loggerFactory)
  private val crypto = TestingIdentityFactory.newCrypto(loggerFactory)(participant1)
  private val publicKey =
    config
      .NonNegativeFiniteDuration(10.seconds)
      .await("get public key")(crypto.cryptoPublicStore.signingKeys.value)
      .valueOrFail("signing keys")
      .headOption
      .value
  private val namespace = Namespace(publicKey.id)
  private val domain = DomainAlias.tryCreate("target")
  private val transactions =
    Seq[TopologyMappingX](
      NamespaceDelegationX.tryCreate(namespace, publicKey, isRootDelegation = true),
      IdentifierDelegationX(UniqueIdentifier(Identifier.tryCreate("alpha"), namespace), publicKey),
      IdentifierDelegationX(UniqueIdentifier(Identifier.tryCreate("beta"), namespace), publicKey),
      IdentifierDelegationX(UniqueIdentifier(Identifier.tryCreate("gamma"), namespace), publicKey),
      IdentifierDelegationX(UniqueIdentifier(Identifier.tryCreate("delta"), namespace), publicKey),
    ).map(txAddFromMapping)
  private val slice1 = transactions.slice(0, 2)
  private val slice2 = transactions.slice(slice1.length, transactions.length)

  private def mk(
      expect: Int,
      responses: Iterator[TopologyTransactionsBroadcastX.State] =
        Iterator.continually(TopologyTransactionsBroadcastX.State.Accepted),
      rejections: Iterator[Option[TopologyTransactionRejection]] = Iterator.continually(None),
  ) = {
    val source = new InMemoryTopologyStoreX(
      TopologyStoreId.AuthorizedStore,
      loggerFactory,
      timeouts,
    )
    val target = new InMemoryTopologyStoreX(
      TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
      loggerFactory,
      timeouts,
    )
    val manager = new AuthorizedTopologyManagerX(
      clock,
      crypto,
      source,
      // we don't need the validation logic to run, because we control the outcome of transactions manually
      enableTopologyTransactionValidation = false,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    val client = new StoreBasedDomainTopologyClientX(
      clock,
      domainId,
      protocolVersion = testedProtocolVersion,
      store = target,
      packageDependencies = StoreBasedDomainTopologyClient.NoPackageDependencies,
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

    (source, target, manager, handle, client)
  }

  private class MockHandle(
      expectI: Int,
      responses: Iterator[State],
      store: TopologyStoreX[TopologyStoreId],
      targetClient: StoreBasedDomainTopologyClientX,
      rejections: Iterator[Option[TopologyTransactionRejection]] = Iterator.continually(None),
  ) extends RegisterTopologyTransactionHandleCommon[
        GenericSignedTopologyTransactionX,
        TopologyTransactionsBroadcastX.State,
      ] {

    val buffer: ListBuffer[GenericSignedTopologyTransactionX] = ListBuffer()
    private val promise = new AtomicReference[Promise[Unit]](Promise[Unit]())
    private val expect = new AtomicInteger(expectI)

    override def submit(
        transactions: Seq[GenericSignedTopologyTransactionX]
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Seq[TopologyTransactionsBroadcastX.State]] =
      FutureUnlessShutdown.outcomeF {
        logger.debug(s"Observed ${transactions.length} transactions")
        buffer ++= transactions
        val finalResult = transactions.map(_ => responses.next())
        for {
          _ <- MonadUtil.sequentialTraverse(transactions)(x => {
            logger.debug(s"Processing $x")
            val ts = CantonTimestamp.now()
            if (finalResult.forall(_ == State.Accepted))
              store
                .update(
                  SequencedTime(ts),
                  EffectiveTime(ts),
                  additions = List(ValidatedTopologyTransactionX(x, rejections.next())),
                  // dumbed down version of how to "append" ValidatedTopologyTransactionXs:
                  removeMapping = Option
                    .when(x.transaction.op == TopologyChangeOpX.Remove)(
                      x.transaction.mapping.uniqueKey
                    )
                    .toList
                    .toSet,
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
                    .onShutdown(())
                )
            else Future.unit
          })
          _ = if (buffer.length >= expect.get()) {
            promise.get().success(())
          }
        } yield {
          logger.debug(s"Done with observed ${transactions.length} transactions")
          finalResult
        }
      }

    def clear(expectI: Int): Seq[GenericSignedTopologyTransactionX] = {
      val ret = buffer.toList
      buffer.clear()
      expect.set(expectI)
      promise.set(Promise())
      ret
    }

    def allObserved(): Future[Unit] = promise.get().future

    override protected def timeouts: ProcessingTimeout = ProcessingTimeout()
    override protected def logger: TracedLogger = StoreBasedDomainOutboxXTest.this.logger
  }

  private def push(
      manager: AuthorizedTopologyManagerX,
      transactions: Seq[GenericTopologyTransactionX],
  ): Future[
    Either[TopologyManagerError, Seq[GenericSignedTopologyTransactionX]]
  ] =
    MonadUtil
      .sequentialTraverse(transactions)(tx =>
        manager.proposeAndAuthorize(
          tx.op,
          tx.mapping,
          tx.serial.some,
          signingKeys = Seq(publicKey.fingerprint),
          testedProtocolVersion,
          expectFullAuthorization = false,
        )
      )
      .value
      .failOnShutdown

  private def outboxConnected(
      manager: AuthorizedTopologyManagerX,
      handle: RegisterTopologyTransactionHandleCommon[
        GenericSignedTopologyTransactionX,
        TopologyTransactionsBroadcastX.State,
      ],
      client: DomainTopologyClientWithInit,
      source: TopologyStoreX[TopologyStoreId.AuthorizedStore],
      target: TopologyStoreX[TopologyStoreId.DomainStore],
  ): Future[StoreBasedDomainOutboxX] = {
    val domainOutbox = new StoreBasedDomainOutboxX(
      domain,
      domainId,
      participant1,
      testedProtocolVersion,
      handle,
      client,
      source,
      target,
      timeouts,
      loggerFactory,
      crypto,
    )
    domainOutbox
      .startup()
      .fold[StoreBasedDomainOutboxX](
        s => fail(s"Failed to start domain outbox $s"),
        _ =>
          domainOutbox.tap(outbox =>
            // add the outbox as an observer since these unit tests avoid instantiating the ParticipantTopologyDispatcher
            manager.addObserver(new TopologyManagerObserver {
              override def addedNewTransactions(
                  timestamp: CantonTimestamp,
                  transactions: Seq[GenericSignedTopologyTransactionX],
              )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
                val num = transactions.size
                outbox.newTransactionsAddedToAuthorizedStore(timestamp, num)
              }
            })
          ),
      )
      .onShutdown(domainOutbox)
  }

  private def outboxDisconnected(manager: AuthorizedTopologyManagerX): Unit =
    manager.clearObservers()

  private def txAddFromMapping(mapping: TopologyMappingX) =
    TopologyTransactionX(
      TopologyChangeOpX.Replace,
      serial = PositiveInt.one,
      mapping,
      testedProtocolVersion,
    )

  private def headTransactions(store: TopologyStoreX[?]) = store
    .findPositiveTransactions(
      asOf = CantonTimestamp.MaxValue,
      asOfInclusive = false,
      isProposal = false,
      types = TopologyMappingX.Code.all,
      filterUid = None,
      filterNamespace = None,
    )
    .map(x => StoredTopologyTransactionsX(x.result.filter(_.validUntil.isEmpty)))

  "dispatcher" should {
    "dispatch transaction on new connect" in {
      val (source, target, manager, handle, client) =
        mk(transactions.length)
      for {
        res <- push(manager, transactions)
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[?]]
        handle.buffer should have length transactions.length.toLong
      }
    }

    "dispatch transaction on existing connections" in {
      val (source, target, manager, handle, client) =
        mk(transactions.length)
      for {
        _ <- outboxConnected(manager, handle, client, source, target)
        res <- push(manager, transactions)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[?]]
        handle.buffer should have length transactions.length.toLong
      }
    }

    "dispatch transactions continuously" in {
      val (source, target, manager, handle, client) = mk(slice1.length)
      for {
        _res <- push(manager, slice1)
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- handle.allObserved()
        observed1 = handle.clear(slice2.length)
        _ <- push(manager, slice2)
        _ <- handle.allObserved()
      } yield {
        observed1.map(_.transaction) shouldBe slice1
        handle.buffer.map(_.transaction) shouldBe slice2
      }
    }

    "not dispatch old data when reconnected" in {
      val (source, target, manager, handle, client) = mk(slice1.length)
      for {
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- push(manager, slice1)
        _ <- handle.allObserved()
        _ = handle.clear(slice2.length)
        _ = outboxDisconnected(manager)
        res2 <- push(manager, slice2)
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- handle.allObserved()
      } yield {
        res2.value shouldBe a[Seq[?]]
        handle.buffer.map(_.transaction) shouldBe slice2
      }
    }

    "correctly find a remove in source store" in {

      val (source, target, manager, handle, client) =
        mk(transactions.length)

      val midRevert = transactions(2).reverse
      val another =
        txAddFromMapping(
          IdentifierDelegationX(
            UniqueIdentifier(Identifier.tryCreate("eta"), namespace),
            publicKey,
          )
        )

      for {
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- push(manager, transactions)
        _ <- handle.allObserved()
        _ = outboxDisconnected(manager)
        // add a remove and another add
        _ <- push(manager, Seq(midRevert, another))
        // ensure that topology manager properly processed this state
        ais <- headTransactions(source).map(_.toTopologyState)
        _ = ais should not contain midRevert.mapping
        _ = ais should contain(another.mapping)
        // and ensure both are not in the new store
        tis <- headTransactions(target).map(_.toTopologyState)
        _ = tis should contain(midRevert.mapping)
        _ = tis should not contain another.mapping
        // re-connect
        _ = handle.clear(2)
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- handle.allObserved()
        tis <- headTransactions(target).map(_.toTopologyState)
      } yield {
        tis should not contain midRevert.mapping
        tis should contain(another.mapping)
      }
    }

    "also push deprecated transactions" in {
      val (source, target, manager, handle, client) =
        mk(transactions.length - 1)
      val midRevertSerialBumped = transactions(2).reverse
      for {
        res <- push(manager, transactions :+ midRevertSerialBumped)
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[?]]
        handle.buffer.map(x =>
          (
            x.transaction.op,
            x.transaction.mapping.maybeUid.map(_.id),
          )
        ) shouldBe Seq(
          (Replace, None),
          (Replace, Some("alpha")),
          (Replace, Some("beta")),
          (Replace, Some("gamma")),
          (Replace, Some("delta")),
          (Remove, Some("beta")),
        )
        handle.buffer should have length 6
      }
    }

    "handle rejected transactions" in {
      val (source, target, manager, handle, client) =
        mk(
          transactions.size,
          rejections = Iterator.continually(Some(TopologyTransactionRejection.NotAuthorized)),
        )
      for {
        _ <- outboxConnected(manager, handle, client, source, target)
        res <- push(manager, transactions)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[?]]
        handle.buffer should have length transactions.length.toLong
      }
    }

    "handle failed transactions" in {
      val (source, target, manager, handle, client) =
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

      @nowarn val Seq(tx1) = transactions.take(1)
      @nowarn val Seq(tx2) = transactions.slice(1, 2)

      lazy val action = for {
        _ <- outboxConnected(manager, handle, client, source, target)
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
        _.errorMessage should include("failed the following topology transactions"),
      )
    }

  }
}
