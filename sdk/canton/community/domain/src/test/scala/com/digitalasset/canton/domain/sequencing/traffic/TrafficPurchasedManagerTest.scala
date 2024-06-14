// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import cats.syntax.parallel.*
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.domain.sequencing.traffic.TrafficPurchasedManager.TrafficPurchasedAlreadyPruned
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.InMemoryTrafficPurchasedStore
import com.digitalasset.canton.lifecycle.PromiseUnlessShutdown
import com.digitalasset.canton.sequencing.traffic.TrafficPurchased
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.CountDownLatch
import scala.concurrent.Future

class TrafficPurchasedManagerTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterEach {
  private val store = new InMemoryTrafficPurchasedStore(loggerFactory)
  private val member = DefaultTestIdentities.participant1.member

  private val clock = new SimClock(loggerFactory = loggerFactory)
  private val timestamp = clock.now

  private def mkManager = new TrafficPurchasedManager(
    store,
    clock,
    SequencerTrafficConfig(
      pruningRetentionWindow = NonNegativeFiniteDuration.ofSeconds(2),
      trafficPurchasedCacheSizePerMember = PositiveInt.one,
    ),
    futureSupervisor,
    SequencerMetrics.noop("traffic-balance-manager"),
    protocolVersion = testedProtocolVersion,
    timeouts,
    loggerFactory,
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    store.close()
    clock.reset()
  }

  private def assertBalance(
      manager: TrafficPurchasedManager,
      timestamp: CantonTimestamp,
      balance: TrafficPurchased,
  ) = {
    manager
      .getTrafficPurchasedAt(member, timestamp, warnIfApproximate = false)
      .value
      .futureValueUS shouldBe Right(
      Some(balance)
    )
  }

  private def assertEmptyBalance(
      manager: TrafficPurchasedManager,
      timestamp: CantonTimestamp,
  ) = {
    manager.getTrafficPurchasedAt(member, timestamp).value.futureValueUS shouldBe Right(None)
  }

  private def assertFailed(manager: TrafficPurchasedManager, timestamp: CantonTimestamp) = {
    manager.getTrafficPurchasedAt(member, timestamp).value.futureValueUS shouldBe Left(
      TrafficPurchasedAlreadyPruned(member, timestamp)
    )
  }

  private def mkBalance(serial: Int, balance: Long, timestamp: CantonTimestamp = timestamp) =
    TrafficPurchased(
      member,
      PositiveInt.tryCreate(serial),
      NonNegativeLong.tryCreate(balance),
      timestamp,
    )

  private val balance = mkBalance(1, 100)

  "TrafficPurchasedManager" should {
    "add and retrieve balances" in {
      val manager = mkManager
      val balance2 = mkBalance(2, 200, timestamp.plusSeconds(1))
      manager.addTrafficPurchased(balance).futureValue
      manager.addTrafficPurchased(balance2).futureValue
      // Avoids a warning when getting the balance at timestamp.plusSeconds(1).immediateSuccessor
      assertBalance(manager, timestamp.immediateSuccessor, balance)
      assertBalance(manager, timestamp.plusSeconds(1).immediateSuccessor, balance2)
    }

    "not fail if requesting a balance before the first buy" in {
      val manager = mkManager
      val balance2 = mkBalance(1, 200, timestamp.plusSeconds(1))
      manager.addTrafficPurchased(balance2).futureValue
      assertEmptyBalance(manager, timestamp.immediateSuccessor)
    }

    "update a balance with a new serial and same timestamp" in {
      val manager = mkManager
      val balance2 = mkBalance(2, 200, timestamp)
      manager.addTrafficPurchased(balance).futureValue
      manager.addTrafficPurchased(balance2).futureValue
      manager
        .getTrafficPurchasedAt(member, timestamp.immediateSuccessor)
        .value
        .futureValueUS shouldBe Right(Some(balance2))
      store.lookup(member).futureValue.find(_.sequencingTimestamp == timestamp) shouldBe Some(
        balance2
      )
    }

    "properly sync between getting a balance and receiving an update for the same timestamp" in {
      val countDownLatch = new CountDownLatch(1)
      var reachedMakePromiseForBalance = false
      val manager = new TrafficPurchasedManager(
        store,
        clock,
        SequencerTrafficConfig(pruningRetentionWindow = NonNegativeFiniteDuration.ofSeconds(2)),
        futureSupervisor,
        SequencerMetrics.noop("traffic-balance-manager"),
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      ) {
        override private[traffic] def makePromiseForBalance(
            member: Member,
            desired: CantonTimestamp,
            lastSeen: CantonTimestamp,
        )(implicit traceContext: TraceContext): Option[PromiseUnlessShutdown[
          Either[TrafficPurchasedManager.TrafficPurchasedManagerError, Option[TrafficPurchased]]
        ]] = {
          reachedMakePromiseForBalance = true
          countDownLatch.await()
          super.makePromiseForBalance(member, desired, lastSeen)
        }
      }
      manager.addTrafficPurchased(balance).futureValue
      val getBalanceF = Future(
        manager.getTrafficPurchasedAt(
          member,
          timestamp.plusSeconds(1),
          lastSeenO = Some(timestamp.plusSeconds(1)),
        )
      )(parallelExecutionContext)
      eventually() {
        reachedMakePromiseForBalance shouldBe true
      }
      val balance2 = mkBalance(2, 200, timestamp.plusSeconds(1))
      // Await on the future, this ensures that we've dequeued pending updates up to timestamp.plusSeconds(1),
      // but we haven't created the promise for it yet
      manager.addTrafficPurchased(balance2).futureValue
      countDownLatch.countDown()
      getBalanceF.futureValue.valueOrFailShutdown("getting balance").futureValue shouldBe Some(
        balance
      )
    }

    "return an error if asking for a balance before the safe pruning mark" in {
      val manager = mkManager
      manager.prune(timestamp.plusSeconds(1)).futureValue
      loggerFactory.suppressWarnings(
        manager.getTrafficPurchasedAt(member, timestamp).value.futureValueUS shouldBe Left(
          TrafficPurchasedAlreadyPruned(member, timestamp)
        )
      )
    }

    "discard outdated updates" in {
      val manager = mkManager
      val balance1 = mkBalance(2, 100)
      val balance2 = mkBalance(1, 300, timestamp.plusSeconds(1))
      manager.addTrafficPurchased(balance1).futureValue
      manager.addTrafficPurchased(balance2).futureValue

      assertBalance(manager, timestamp.plusSeconds(1), balance1)
    }

    "return a balance even if the requested timestamp is after the last update, but no updates are in flight" in {
      val manager = mkManager
      manager.addTrafficPurchased(balance).futureValue

      val desiredTimestamp = timestamp.plusSeconds(5)
      val lastSeen = timestamp
      manager
        .getTrafficPurchasedAt(member, desiredTimestamp, Some(lastSeen))
        .value
        .futureValueUS shouldBe Right(
        Some(
          balance
        )
      )
    }

    "log a warning if returning a balance when no lastSeen was provided and the desired timestamp is after the last update" in {
      val manager = mkManager
      manager.addTrafficPurchased(balance).futureValue

      val desiredTimestamp = timestamp.plusSeconds(5)

      val balanceF = loggerFactory.assertLogs(
        manager.getTrafficPurchasedAt(member, desiredTimestamp).value,
        _.warningMessage should include(
          s"The desired timestamp $desiredTimestamp is more recent than the last update $timestamp, and no 'lastSeen' timestamp was provided. The provided balance may not be up to date if a balance update is being processed."
        ),
      )
      balanceF.futureValueUS shouldBe Right(
        Some(balance)
      )
    }

    "wait to see the relevant update before returning the balance if there is an update in flight" in {
      val manager = mkManager
      manager.addTrafficPurchased(balance).futureValue

      val desiredTimestamp = timestamp.plusSeconds(5)
      val lastSeen = timestamp.plusSeconds(1)

      // We should be waiting, so make sure the future does not complete
      val resultF = manager.getTrafficPurchasedAt(member, desiredTimestamp, Some(lastSeen)).value
      always() {
        resultF.isCompleted shouldBe false
      }

      // Update the balance just before lastSeen
      val balance2 = mkBalance(2, 100, lastSeen.immediatePredecessor)
      manager.addTrafficPurchased(balance2).futureValue
      manager.tick(lastSeen)

      // balance2 just became active before last seen, so it should be the one returned
      resultF.futureValueUS shouldBe Right(Some(balance2))
    }

    "prune" in {
      val manager = mkManager
      val t0 = clock.now

      // Add a balance so it's in the in memory balance map
      val balance = mkBalance(1, 100, t0)
      val balance2 = mkBalance(2, 200, t0.plusSeconds(1))
      val balance3 = mkBalance(3, 300, t0.plusSeconds(3))

      manager.addTrafficPurchased(balance).futureValue
      manager.addTrafficPurchased(balance2).futureValue
      manager.addTrafficPurchased(balance3).futureValue

      // Prune t0
      manager.prune(t0.plusSeconds(1)).futureValue
      assertFailed(manager, t0)
      assertBalance(manager, t0.plusSeconds(1).immediateSuccessor, balance2)

      val balance4 = mkBalance(4, 400, t0.plusSeconds(4))
      manager.addTrafficPurchased(balance4).futureValue

      manager.prune(t0.plusSeconds(5)).futureValue
      assertFailed(manager, t0)
      assertFailed(manager, t0.plusSeconds(1))
      assertFailed(manager, t0.plusSeconds(2))
      assertFailed(manager, t0.plusSeconds(3))
      assertBalance(manager, t0.plusSeconds(4).immediateSuccessor, balance4)
      assertBalance(manager, t0.plusSeconds(5), balance4)
    }

    "read and writes can happen concurrently" in {
      val updatesNb = 200
      val now = clock.now
      val updates = (0 to updatesNb).map(i => mkBalance(i + 1, i.toLong, now.plusSeconds(i.toLong)))
      val manager = mkManager

      // Write a bunch of updates - sequentially because otherwise we could have updates be skipped because serials
      // arrive out of order. But don't wait on the future here
      val writeF = MonadUtil.sequentialTraverse_(updates.toList)(manager.addTrafficPurchased)

      // In the meantime, start reading the updates concurrently
      val reads = updates.toList.parTraverse { u =>
        manager.getTrafficPurchasedAt(
          member,
          u.sequencingTimestamp.immediateSuccessor,
          lastSeenO = Some(u.sequencingTimestamp),
        )(TraceContext.empty)
      }

      val res = reads.valueOrFailShutdown("reading balance updates").futureValue

      res.zipWithIndex.foreach { case (b, i) =>
        b shouldBe Some(updates(i))
      }

      timeouts.default.await_("writeF")(writeF)
    }
  }
}
