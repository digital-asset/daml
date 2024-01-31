// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.implicits.*
import com.digitalasset.canton.common.domain.RegisterTopologyTransactionHandleCommon
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.store.memory.InMemoryDamlPackageStore
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponseResult
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponseResult.State
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Add, Remove}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{BaseTest, DomainAlias, config}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.util.chaining.scalaUtilChainingOps

class DomainOutboxTest extends AsyncWordSpec with BaseTest {
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
    Seq[TopologyStateUpdateMapping](
      NamespaceDelegation(namespace, publicKey, isRootDelegation = true),
      IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("alpha"), namespace), publicKey),
      IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("beta"), namespace), publicKey),
      IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("gamma"), namespace), publicKey),
      IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("delta"), namespace), publicKey),
    ).map(TopologyStateUpdate.createAdd(_, testedProtocolVersion))
  private val slice1 = transactions.slice(0, 2)
  private val slice2 = transactions.slice(slice1.length, transactions.length)

  private def mk(expect: Int) = {
    val source = new InMemoryTopologyStore(
      TopologyStoreId.AuthorizedStore,
      loggerFactory,
      timeouts,
      futureSupervisor,
    )
    val target = new InMemoryTopologyStore(
      TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
      loggerFactory,
      timeouts,
      futureSupervisor,
    )
    val packageDependencyResolver = new PackageDependencyResolver(
      new InMemoryDamlPackageStore(loggerFactory),
      timeouts,
      loggerFactory,
    )
    val manager = new ParticipantTopologyManager(
      clock,
      source,
      crypto,
      packageDependencyResolver,
      timeouts,
      testedProtocolVersion,
      loggerFactory,
      futureSupervisor,
    )
    val handle = new MockHandle(expect, store = target)
    val client = mock[DomainTopologyClientWithInit]
    (source, target, manager, handle, client)
  }

  private class MockHandle(
      expectI: Int,
      response: State = State.Accepted,
      store: TopologyStore[TopologyStoreId],
  ) extends RegisterTopologyTransactionHandleCommon[SignedTopologyTransaction[
        TopologyChangeOp
      ], RegisterTopologyTransactionResponseResult.State] {
    val buffer: mutable.ListBuffer[SignedTopologyTransaction[TopologyChangeOp]] = ListBuffer()
    private val promise = new AtomicReference[Promise[Unit]](Promise[Unit]())
    private val expect = new AtomicInteger(expectI)

    override def submit(
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Seq[RegisterTopologyTransactionResponseResult.State]] =
      FutureUnlessShutdown.outcomeF {
        logger.debug(s"Observed ${transactions.length} transactions")
        buffer ++= transactions
        for {
          _ <- MonadUtil.sequentialTraverse(transactions)(x => {
            logger.debug(s"Adding $x")
            val ts = CantonTimestamp.now()
            store.append(
              SequencedTime(ts),
              EffectiveTime(ts),
              List(ValidatedTopologyTransaction(x, None)),
            )
          })
          _ = if (buffer.length >= expect.get()) {
            promise.get().success(())
          }
        } yield {
          logger.debug(s"Done with observed ${transactions.length} transactions")
          transactions.map(_ => response)
        }
      }

    def clear(expectI: Int): Seq[SignedTopologyTransaction[TopologyChangeOp]] = {
      val ret = buffer.toList
      buffer.clear()
      expect.set(expectI)
      promise.set(Promise())
      ret
    }

    def allObserved(): Future[Unit] = promise.get().future

    override protected def timeouts: ProcessingTimeout = ProcessingTimeout()
    override protected def logger: TracedLogger = DomainOutboxTest.this.logger
  }

  private def push(
      manager: ParticipantTopologyManager,
      transactions: Seq[TopologyTransaction[TopologyChangeOp]],
  ): Future[
    Either[ParticipantTopologyManagerError, Seq[SignedTopologyTransaction[TopologyChangeOp]]]
  ] =
    MonadUtil
      .sequentialTraverse(transactions)(x =>
        manager.authorize(x, Some(publicKey.fingerprint), testedProtocolVersion)
      )
      .value
      .failOnShutdown

  private def outboxConnected(
      manager: ParticipantTopologyManager,
      handle: RegisterTopologyTransactionHandleCommon[SignedTopologyTransaction[
        TopologyChangeOp
      ], RegisterTopologyTransactionResponseResult.State],
      client: DomainTopologyClientWithInit,
      source: TopologyStore[TopologyStoreId.AuthorizedStore],
      target: TopologyStore[TopologyStoreId.DomainStore],
  ): Future[StoreBasedDomainOutbox] = {
    val domainOutbox = new StoreBasedDomainOutbox(
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
      futureSupervisor = FutureSupervisor.Noop,
    )
    domainOutbox
      .startup()
      .fold[StoreBasedDomainOutbox](
        s => fail(s"Failed to start domain outbox $s"),
        _ =>
          domainOutbox.tap(outbox =>
            // add the outbox as an observer since these unit tests avoid instantiating the ParticipantTopologyDispatcher
            manager.addObserver(new ParticipantTopologyManagerObserver {
              override def addedNewTransactions(
                  timestamp: CantonTimestamp,
                  transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
              )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
                val num = transactions.size
                outbox.newTransactionsAddedToAuthorizedStore(timestamp, num)
              }
            })
          ),
      )
      .onShutdown(domainOutbox)
  }

  private def outboxDisconnected(manager: ParticipantTopologyManager): Unit =
    manager.clearObservers()

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
        TopologyStateUpdate.createAdd(
          IdentifierDelegation(UniqueIdentifier(Identifier.tryCreate("eta"), namespace), publicKey),
          testedProtocolVersion,
        )

      for {
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- push(manager, transactions)
        _ <- handle.allObserved()
        _ = outboxDisconnected(manager)
        // add a remove and another add
        _ <- push(manager, Seq(midRevert, another))
        // ensure that topology manager properly processed this state
        ais <- source.headTransactions.map(_.toTopologyState)
        _ = ais should not contain midRevert.element
        _ = ais should contain(another.element)
        // and ensure both are not in the new store
        tis <- target.headTransactions.map(_.toTopologyState)
        _ = tis should contain(midRevert.element)
        _ = tis should not contain another.element
        // re-connect
        _ = handle.clear(2)
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- handle.allObserved()
        tis <- target.headTransactions.map(_.toTopologyState)
      } yield {
        tis should not contain midRevert.element
        tis should contain(another.element)
      }
    }

    "not push deprecated transactions" in {
      val (source, target, manager, handle, client) =
        mk(transactions.length - 1)
      val midRevert = transactions(2).reverse
      for {
        res <- push(manager, transactions :+ midRevert)
        _ <- outboxConnected(manager, handle, client, source, target)
        _ <- handle.allObserved()
      } yield {
        res.value shouldBe a[Seq[?]]
        handle.buffer.map(x =>
          (
            x.transaction.op,
            x.transaction.element.uniquePath.maybeUid.map(_.id),
          )
        ) shouldBe Seq(
          (Add, None),
          (Add, Some("alpha")),
          (Add, Some("gamma")),
          (Add, Some("delta")),
          (Remove, Some("beta")),
        )
        handle.buffer should have length 5
      }
    }

  }
}
