// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.cache

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.BatchAggregatorConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.metrics.CacheMetrics
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionTestFactory,
}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.TopologyStore.{
  StateKeyFetch,
  TopologyStoreDeactivations,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  NamespaceDelegation,
  ParticipantPermission,
  PartyToParticipant,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, Namespace, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.language.implicitConversions

object CacheTestMetrics {
  val metrics = new CacheMetrics("test", new NoOpMetricsFactory)
}

class TopologyStateWriteThroughCacheTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasTestCloseContext {

  private implicit def toEffectiveTime(ts: CantonTimestamp): EffectiveTime = EffectiveTime(ts)
  private implicit def toSequencedTime(ts: CantonTimestamp): SequencedTime = SequencedTime(ts)

  private def wrapAppend[T](wrap: => T): T =
    loggerFactory.assertLogs(
      wrap,
      // append will access a state which we haven't fetched yet.
      // this is correct but inefficient. therefore we need to ignore this
      // warning in this test.
      _.warningMessage should include("Trying to access topology state"),
    )

  private def unpack[T](res: UnlessShutdown[T]): T = res match {
    case UnlessShutdown.Outcome(result) => result
    case UnlessShutdown.AbortedDueToShutdown => fail("shouldn't have shutdown here")
  }

  class TestFixture(val testName: String, testPos: Int)
      extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext) {
    private[cache] val loggerFactoryExt =
      loggerFactory.appendUnnamedKey("testPos", testPos.toString)
    private[cache] val logger = loggerFactoryExt.getLogger(getClass)

    private[cache] val store = mock[TopologyStore[TopologyStoreId]]
    when(store.storeId).thenReturn(
      TopologyStoreId.SynchronizerStore(DefaultTestIdentities.physicalSynchronizerId)
    )
    private[cache] val fetchSync =
      new AtomicReference[List[
        (
            PromiseUnlessShutdown[Seq[StateKeyFetch]],
            PromiseUnlessShutdown[Unit],
        )
      ]](
        List.empty
      )
    private[cache] val updateSync = new AtomicReference[
      PromiseUnlessShutdown[(TopologyStoreDeactivations, Seq[GenericValidatedTopologyTransaction])]
    ](
      PromiseUnlessShutdown.unsupervised()
    )
    private[cache] def syncWrite(): FutureUnlessShutdown[
      (TopologyStoreDeactivations, Seq[GenericValidatedTopologyTransaction])
    ] = {
      val updateP = updateSync.get()
      updateP.futureUS.map { res =>
        updateSync.set(PromiseUnlessShutdown.unsupervised())
        res
      }
    }

    private[cache] def expect(requests: Seq[GenericStoredTopologyTransaction]*) = {

      result.set(requests.map(_.sortBy(_.validFrom).reverse))
      val invoked = List.fill(requests.length)(
        PromiseUnlessShutdown.unsupervised[Seq[StateKeyFetch]]()
      )

      val completed = List.fill(requests.length)(PromiseUnlessShutdown.unsupervised[Unit]())
      fetchSync.set(invoked.zip(completed))
      invoked.map(_.futureUS).zip(completed)
    }

    private[cache] def ts(off: Int): CantonTimestamp = CantonTimestamp.Epoch.plusMillis(off.toLong)

    private[cache] def toStored(
        tx: GenericSignedTopologyTransaction,
        from: CantonTimestamp,
        until: Option[CantonTimestamp],
    ): GenericStoredTopologyTransaction =
      StoredTopologyTransaction(
        from,
        from,
        until.map(EffectiveTime(_)),
        tx,
        None,
      )

    private[cache] def next(
        cur: AtomicReference[List[
          (
              PromiseUnlessShutdown[Seq[StateKeyFetch]],
              PromiseUnlessShutdown[Unit],
          )
        ]]
    ) =
      cur.getAndUpdate {
        case _ :: next => next
        case Nil => fail("No more fake promises!")
      } match {
        case head :: _ => head
        case Nil => fail("No more fake promises!")
      }

    val result = new AtomicReference[Seq[Seq[GenericStoredTopologyTransaction]]](Seq.empty)
    when(
      store.fetchAllDescending(anySeq[StateKeyFetch])(
        any[TraceContext]
      )
    )
      .thenAnswer[Seq[StateKeyFetch], TraceContext] { case (items, _) =>
        logger.debug(s"Fetch of $items")
        // get current result future
        val (invoked, completed) = next(fetchSync)
        // fetch payload and reset result
        val allTxs = result.getAndUpdate(_.drop(1)).headOption.getOrElse(Seq.empty)
        // mark invoked
        invoked.outcome_(items)
        completed.futureUS.map { _ =>
          StoredTopologyTransactions(allTxs)
        }
      }
    when(
      store.update(
        any[SequencedTime],
        any[EffectiveTime],
        any[TopologyStoreDeactivations],
        any[Seq[GenericValidatedTopologyTransaction]],
      )(anyTraceContext)
    ).thenAnswer[SequencedTime, EffectiveTime, TopologyStoreDeactivations, Seq[
      GenericValidatedTopologyTransaction
    ]] { case (_, _, removals, additions) =>
      val promise = updateSync.get()
      promise.outcome_((removals, additions))
      promise.futureUS.map(_ => ())
    }

    val cache = new TopologyStateWriteThroughCache(
      store,
      // only one parallel loader so we can structure the test incrementally
      BatchAggregatorConfig(maximumInFlight = PositiveInt.one),
      cacheEvictionThreshold = PositiveInt.one,
      maxCacheSize = PositiveInt.two,
      enableConsistencyChecks = true,
      CacheTestMetrics.metrics,
      FutureSupervisor.Noop,
      timeouts,
      loggerFactoryExt,
    )(parallelExecutionContext)

    private[cache] def grabNs(
        timestamp: CantonTimestamp,
        ns: Namespace,
    ): Future[Seq[GenericStoredTopologyTransaction]] = {
      logger.debug(s"Grab $ns at $timestamp")
      cache
        .lookupForNamespace(
          EffectiveTime(timestamp),
          asOfInclusive = true,
          ns,
          transactionTypes = Set(Code.NamespaceDelegation),
        )
        .onShutdown(Seq())
    }

    private[cache] def grabUid(
        timestamp: CantonTimestamp,
        uid: UniqueIdentifier,
    ): Future[Seq[GenericStoredTopologyTransaction]] = {
      logger.debug(s"Grab $uid at $timestamp")
      cache
        .lookupForUid(
          EffectiveTime(timestamp),
          asOfInclusive = true,
          uid,
          transactionTypes = Set(Code.OwnerToKeyMapping),
        )
        .onShutdown(Seq())
    }

  }

  override type FixtureParam = TestFixture

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val testPos = test.pos.map(_.lineNumber).getOrElse(0)
    val f = new TestFixture(test.name, testPos)
    withFixture(test.toNoArgAsyncTest(f))
  }

  "topology-write-through cache" when {

    "loading" should {
      "correctly return existing data including history fetches" in { f =>
        import f.*
        val tx1 = toStored(ns1k1_k1, ts(0), None)

        val tx2 = toStored(okm1bk5k1E_k2, ts(0), Some(ts(1)))
        val tx3 = toStored(okm1bk5k1E_k1, ts(1), Some(ts(5)))

        expect(Seq(tx1), Seq.empty, Seq(tx3), Seq(tx2, tx3)).foreach(_._2.outcome_(()))

        for {
          c1 <- grabNs(ts(0), ns1)
          c2 <- grabNs(ts(1), ns2)
          // this one will only load the history up until ts(2), omitting tx3
          c3 <- grabUid(ts(2), okm1bk5k1E_k1.mapping.member.uid)
          // this one will now trigger the loading of tx2
          c4 <- grabUid(ts(0), okm1bk5k1E_k1.mapping.member.uid)
          c5 <- grabUid(ts(7), okm1bk5k1E_k1.mapping.member.uid)
        } yield {
          c1 shouldBe Seq(tx1)
          c2 shouldBe empty
          c3 shouldBe Seq(tx3)
          c4 shouldBe Seq(tx2) // should return history lookup
          c5 shouldBe empty
        }

      }

      "synchronise on multiple fetches" in { f =>
        import f.*
        val tx1 = toStored(ns1k1_k1, ts(0), None)

        // only expect one fetch invocation
        val (invoked, completed) = expect(Seq(tx1)) match {
          case one :: Nil => one
          case _ => fail("bad")
        }

        val currentF1 = grabNs(ts(0), ns1)
        val currentF2 = grabNs(ts(1), ns1)

        for {
          // wait until fetch is invoked
          _ <- invoked.failOnShutdown
          currentF3 = grabNs(ts(2), ns1)
          _ = completed.outcome_(())
          c1 <- currentF1
          c2 <- currentF2
          c3 <- currentF3
        } yield {
          c1 shouldBe Seq(tx1)
          c2 shouldBe Seq(tx1)
          c3 shouldBe Seq(tx1)
        }

      }

    }

    "updating" when {

      "add and flush" in { f =>
        import f.*
        // expect one fetch
        expect(Seq.empty).foreach { case (_, completedP) => completedP.outcome_(()) }
        val addF = wrapAppend(
          cache
            .append(ts(0), ts(0), ValidatedTopologyTransaction(ns1k1_k1))
            .onShutdown(())
        )
        for {
          _ <- addF
          c1 <- grabNs(ts(1), ns1)
          _ = cache.flush(ts(0), ts(0))
          stored <- f.updateSync.get().future.map(unpack)
        } yield {
          c1.map(_.transaction) shouldBe Seq(ns1k1_k1)
          val (removals, adds) = stored
          removals shouldBe empty
          adds.map(_.transaction) shouldBe Seq(ns1k1_k1)
        }

      }

      "replace" in { f =>
        import f.*
        // start with one in the store

        val ns1k1_k1_remove = mkRemoveTx(ns1k1_k1)
        expect(Seq(toStored(ns1k1_k1, ts(0), None))).foreach { case (_, completedP) =>
          completedP.outcome_(())
        }
        val addAndRemoveF = wrapAppend(
          cache
            .append(ts(1), ts(1), ValidatedTopologyTransaction(ns1k1_k1_remove))
        )

        (for {
          _ <- addAndRemoveF
          _ <- cache.flush(ts(0), ts(0))
          stored <- f.syncWrite()
        } yield {
          val (removed, added) = stored
          // we should see the archival
          removed.get(ns1k1_k1.mapping.uniqueKey) should contain((Some(PositiveInt.one), Set.empty))
          // we should see the remove tx
          added.map(c => (c.transaction, c.expireImmediately)) shouldBe Seq(
            (ns1k1_k1_remove, false)
          )
        }).failOnShutdown
      }

      "in-batch update" in { f =>
        import f.*

        val p1p1_k1_s1 =
          mkAdd(
            PartyToParticipant.tryCreate(
              party1b,
              threshold = PositiveInt.one,
              Seq(HostingParticipant(participant1, ParticipantPermission.Submission)),
            ),
            SigningKeys.key1,
          )
        val p1p1_k1_s2 =
          mkAdd(
            PartyToParticipant.tryCreate(
              party1b,
              threshold = PositiveInt.one,
              Seq(HostingParticipant(participant1, ParticipantPermission.Submission)),
            ),
            signingKey = SigningKeys.key1,
            serial = PositiveInt.two,
          )

        val p1p1_k1_s3 = mkRemoveTx(p1p1_k1_s2)

        expect(Seq.empty).foreach { case (_, completedP) => completedP.outcome_(()) }
        (for {
          _ <- wrapAppend(
            cache.append(ts(0), ts(0), ValidatedTopologyTransaction(p1p1_k1_s1, None))
          )
          _ <- cache.append(ts(0), ts(0), ValidatedTopologyTransaction(p1p1_k1_s2, None))
          _ <- cache.flush(ts(0), ts(0))
          resultAtT0 <- syncWrite()
          _ <- cache.append(ts(1), ts(1), ValidatedTopologyTransaction(p1p1_k1_s3, None))
          _ <- cache.flush(ts(1), ts(1))
          resultAtT1 <- syncWrite()
        } yield {
          val (remove, adds) = resultAtT0
          remove shouldBe empty
          adds.map(c => (c.transaction, c.expireImmediately)) shouldBe Seq(
            (p1p1_k1_s1, true),
            (p1p1_k1_s2, false),
          )
          val (remove2, adds2) = resultAtT1
          remove2 shouldBe Map(p1p1_k1_s3.mapping.uniqueKey -> (Some(PositiveInt.two), Set.empty))
          adds2.map(c => (c.transaction, c.expireImmediately)) shouldBe Seq((p1p1_k1_s3, false))
          succeed
        }).failOnShutdown
      }

    }

    "proposal update and expiry" in { f =>
      import f.*

      def mkProposal(permission: ParticipantPermission) =
        mkAdd(
          PartyToParticipant.tryCreate(
            party1b,
            threshold = PositiveInt.one,
            Seq(
              HostingParticipant(participant6, permission)
            ),
          ),
          SigningKeys.key1,
          isProposal = true,
          serial = PositiveInt.one,
        )

      val p1p1_k1_s1 = mkAddMultiKey(
        PartyToParticipant.tryCreate(
          party1b,
          threshold = PositiveInt.one,
          Seq(
            HostingParticipant(participant6, ParticipantPermission.Observation)
          ),
        ),
        signingKeys = NonEmpty.mk(Set, SigningKeys.key1, SigningKeys.key6),
        serial = PositiveInt.one,
      )
      val p1p1_k1_s2_p = mkProposal(ParticipantPermission.Submission)
      val p1p1_k1_s2_p2 = mkProposal(ParticipantPermission.Confirmation)

      val p1p1_k1_s2 = mkAddMultiKey(
        p1p1_k1_s2_p.mapping,
        signingKeys = NonEmpty.mk(Set, SigningKeys.key1, SigningKeys.key6),
        serial = PositiveInt.two,
      )

      expect(Seq(toStored(p1p1_k1_s1, ts(-1), None), toStored(p1p1_k1_s2_p, ts(0), None))).foreach {
        case (_, completedP) => completedP.outcome_(())
      }

      (for {
        _ <- wrapAppend(
          cache.append(ts(1), ts(1), ValidatedTopologyTransaction(p1p1_k1_s2_p2, None))
        )
        _ <- cache.flush(ts(1), ts(1))
        res <- syncWrite()
        // duplicate
        _ <- cache.append(
          ts(2),
          ts(2),
          ValidatedTopologyTransaction(p1p1_k1_s2_p2, None),
        )
        _ <- cache.flush(ts(2), ts(2))
        res2 <- syncWrite()
        // actual tx
        _ <- cache.append(ts(3), ts(3), ValidatedTopologyTransaction(p1p1_k1_s2, None))
        _ <- cache.flush(ts(3), ts(3))
        res3 <- syncWrite()
      } yield {
        val (remove1, add1) = res
        remove1 shouldBe empty
        add1.map(_.transaction) shouldBe Seq(p1p1_k1_s2_p2)
        val (remove2, add2) = res2
        remove2 shouldBe Map(
          p1p1_k1_s2_p.mapping.uniqueKey -> (None, Set(p1p1_k1_s2_p2.transaction.hash))
        )
        add2.map(_.transaction) shouldBe Seq(p1p1_k1_s2_p2)
        val (remove3, add3) = res3
        remove3 shouldBe Map(
          p1p1_k1_s2_p.mapping.uniqueKey -> (Some(PositiveInt.one), Set())
        )
        add3.map(_.transaction) shouldBe Seq(p1p1_k1_s2)
      }).failOnShutdown

    }

    // eviction
    "eviction" when {
      "keep below threshold" in { f =>
        import f.*

        val ((invoke1, outcome1), (invoke2, outcome2)) = expect(
          Seq(toStored(ns1k1_k1, ts(0), None)),
          Seq(
            toStored(ns2k2_k2, ts(0), None),
            toStored(ns3k3_k3, ts(0), None),
            toStored(ns6k6_k6, ts(0), None),
          ),
        ) match {
          case one :: two :: Nil => (one, two)
          case _ => fail("invalid")
        }

        // load all parallel (batch aggregator should load in two batches)
        val c1F = grabNs(ts(0), ns1)
        val c2F = grabNs(ts(0), ns2)
        val c3F = grabNs(ts(0), ns3)
        val c4F = grabNs(ts(1), ns1)

        for {
          // flush loading
          fetch1 <- invoke1.failOnShutdown
          _ = outcome1.outcome_(())
          // sync on first load
          c1 <- c1F
          // flush rest
          fetch2 <- invoke2.failOnShutdown
          // run eviction during loading of rest (will not do anything, because so far only ns1 is loaded)
          cacheSizeL = cache.evict()
          _ = cacheSizeL shouldBe 1
          // now, complete second load
          _ = outcome2.outcome_(())
          // all reads should resolve now
          c2 <- c2F
          c3 <- c3F
          c4 <- c4F
          // run eviction
          cacheSize = cache.evict()
          cacheSize2 = cache.evict()
        } yield {
          val et = EffectiveTime(ts(0))
          fetch1.toSet shouldBe Set(StateKeyFetch(NamespaceDelegation.code, ns1, None, et))
          fetch2.toSet shouldBe Set(
            StateKeyFetch(NamespaceDelegation.code, ns2, None, et),
            StateKeyFetch(NamespaceDelegation.code, ns3, None, et),
          )
          c1.map(_.transaction) shouldBe Seq(ns1k1_k1)
          c2.map(_.transaction) shouldBe Seq(ns2k2_k2)
          c3.map(_.transaction) shouldBe Seq(ns3k3_k3)
          c4.map(_.transaction) shouldBe Seq(ns1k1_k1)
          cacheSize shouldBe 3 // access flag is still set for ns1, so will not be evicted
          cacheSize2 shouldBe 2 // now, ns1 should be evicted
        }

      }

      "don't start concurrent evictions" in { f =>
        import f.*

        val invokedAndOutcome = expect(
          Seq(toStored(ns1k1_k1, ts(0), None)),
          Seq(
            toStored(ns2k2_k2, ts(0), None),
            toStored(ns3k3_k3, ts(0), None),
            toStored(ns6k6_k6, ts(0), None),
          ),
        )

        // acquire eviction lock
        val lockP = cache.acquireEvictionLock()

        // loaded in first batch
        val c1F = grabNs(ts(0), ns1)
        // grabbed in snd batch
        val c2F = grabNs(ts(0), ns2)
        val c3F = grabNs(ts(0), ns3)
        val c4F = grabNs(ts(0), ns6)
        cache.cacheSize() shouldBe (0, 0)
        f.logger.debug("waiting on eviction lock")
        for {
          unlockP <- lockP.failOnShutdown
          lockP2 = cache.acquireEvictionLock()
          _ = {
            // immediately complete second wait here
            lockP2.map(_.outcome_(()))
          }
          _ = cache.evictIfNecessary()
          // complete first read
          _ = invokedAndOutcome(0)._2.outcome_(())
          _ <- invokedAndOutcome(0)._1.failOnShutdown
          _ <- c1F
          _ = invokedAndOutcome(1)._2.outcome_(())
          _ <- invokedAndOutcome(1)._1.failOnShutdown
          _ <- c2F
          _ <- c3F
          _ = f.logger.debug("cache eviction should not start")
          // this one should fire a cache eviction to bring it down to 2, but this eviction
          // is blocked on us releasing the unlock
          _ <- c4F
          _ = cache.cacheSize() shouldBe (0, 4)
          // second lock should still be pending
          _ = lockP2.isCompleted shouldBe false
          _ = unlockP.outcome_(())
          // next grab should trigger eviction
          _ = f.logger.debug("cache eviction should start now")

        } yield {
          // we might need a few iterations due to race conditions
          eventually() {
            cache.evictIfNecessary()
            cache.cacheSize() shouldBe (2, 0)
          }
          succeed
        }

      }

    }

  }

}
