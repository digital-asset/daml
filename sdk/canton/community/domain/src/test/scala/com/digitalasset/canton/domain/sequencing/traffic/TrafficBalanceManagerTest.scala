// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import cats.syntax.parallel.*
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.domain.sequencing.traffic.TrafficBalanceManager.TrafficBalanceAlreadyPruned
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.InMemoryTrafficBalanceStore
import com.digitalasset.canton.lifecycle.PromiseUnlessShutdown
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.CountDownLatch
import scala.concurrent.Future

class TrafficBalanceManagerTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterEach {
  private val store = new InMemoryTrafficBalanceStore(loggerFactory)
  private val member = DefaultTestIdentities.participant1.member

  private val clock = new SimClock(loggerFactory = loggerFactory)
  private val timestamp = clock.now

  private def mkManager = new TrafficBalanceManager(
    store,
    clock,
    SequencerTrafficConfig(pruningRetentionWindow = NonNegativeFiniteDuration.ofSeconds(2)),
    futureSupervisor,
    SequencerMetrics.noop("traffic-balance-manager"),
    timeouts,
    loggerFactory,
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    store.close()
    clock.reset()
  }

  private def assertBalance(
      manager: TrafficBalanceManager,
      timestamp: CantonTimestamp,
      balance: TrafficBalance,
  ) = {
    manager.getTrafficBalanceAt(member, timestamp).value.futureValueUS shouldBe Right(Some(balance))
  }

  private def assertEmptyBalance(
      manager: TrafficBalanceManager,
      timestamp: CantonTimestamp,
  ) = {
    manager.getTrafficBalanceAt(member, timestamp).value.futureValueUS shouldBe Right(None)
  }

  private def assertFailed(manager: TrafficBalanceManager, timestamp: CantonTimestamp) = {
    manager.getTrafficBalanceAt(member, timestamp).value.futureValueUS shouldBe Left(
      TrafficBalanceAlreadyPruned(member, timestamp)
    )
  }

  private def mkBalance(serial: Int, balance: Long, timestamp: CantonTimestamp = timestamp) =
    TrafficBalance(
      member,
      PositiveInt.tryCreate(serial),
      NonNegativeLong.tryCreate(balance),
      timestamp,
    )

  private val balance = mkBalance(1, 100)

  "TrafficBalanceManager" should {
    "add and retrieve balances" in {
      val manager = mkManager
      val balance2 = mkBalance(2, 200, timestamp.plusSeconds(1))
      manager.addTrafficBalance(balance).futureValue
      manager.addTrafficBalance(balance2).futureValue
      // Avoids a warning when getting the balance at timestamp.plusSeconds(1).immediateSuccessor
      assertBalance(manager, timestamp.immediateSuccessor, balance)
      assertBalance(manager, timestamp.plusSeconds(1).immediateSuccessor, balance2)
    }

    "not fail if requesting a balance before the first buy" in {
      val manager = mkManager
      val balance2 = mkBalance(1, 200, timestamp.plusSeconds(1))
      manager.addTrafficBalance(balance2).futureValue
      assertEmptyBalance(manager, timestamp.immediateSuccessor)
    }

    "update a balance with a new serial and same timestamp" in {
      val manager = mkManager
      val balance2 = mkBalance(2, 200, timestamp)
      manager.addTrafficBalance(balance).futureValue
      manager.addTrafficBalance(balance2).futureValue
      manager
        .getTrafficBalanceAt(member, timestamp.immediateSuccessor)
        .value
        .futureValueUS shouldBe Right(Some(balance2))
      store.lookup(member).futureValue.find(_.sequencingTimestamp == timestamp) shouldBe Some(
        balance2
      )
    }

    "properly sync between getting a balance and receiving an update for the same timestamp" in {
      val countDownLatch = new CountDownLatch(1)
      var reachedMakePromiseForBalance = false
      val manager = new TrafficBalanceManager(
        store,
        clock,
        SequencerTrafficConfig(pruningRetentionWindow = NonNegativeFiniteDuration.ofSeconds(2)),
        futureSupervisor,
        SequencerMetrics.noop("traffic-balance-manager"),
        timeouts,
        loggerFactory,
      ) {
        override private[traffic] def makePromiseForBalance(
            member: Member,
            desired: CantonTimestamp,
            lastSeen: CantonTimestamp,
        )(implicit traceContext: TraceContext): Option[PromiseUnlessShutdown[
          Either[TrafficBalanceManager.TrafficBalanceManagerError, Option[TrafficBalance]]
        ]] = {
          reachedMakePromiseForBalance = true
          countDownLatch.await()
          super.makePromiseForBalance(member, desired, lastSeen)
        }
      }
      manager.addTrafficBalance(balance).futureValue
      val getBalanceF = Future(
        manager.getTrafficBalanceAt(
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
      manager.addTrafficBalance(balance2).futureValue
      countDownLatch.countDown()
      getBalanceF.futureValue.valueOrFailShutdown("getting balance").futureValue shouldBe Some(
        balance
      )
    }

    "return an error if asking for a balance before the safe pruning mark" in {
      val manager = mkManager
      manager.setSafeToPruneBeforeExclusive(timestamp.plusSeconds(1))
      loggerFactory.suppressWarnings(
        manager.getTrafficBalanceAt(member, timestamp).value.futureValueUS shouldBe Left(
          TrafficBalanceAlreadyPruned(member, timestamp)
        )
      )
    }

    "discard outdated updates" in {
      val manager = mkManager
      val balance1 = mkBalance(2, 100)
      val balance2 = mkBalance(1, 300, timestamp.plusSeconds(1))
      manager.addTrafficBalance(balance1).futureValue
      manager.addTrafficBalance(balance2).futureValue

      assertBalance(manager, timestamp.plusSeconds(1), balance1)
    }

    "return a balance even if the requested timestamp is after the last update, but no updates are in flight" in {
      val manager = mkManager
      manager.addTrafficBalance(balance).futureValue

      val desiredTimestamp = timestamp.plusSeconds(5)
      val lastSeen = timestamp
      manager
        .getTrafficBalanceAt(member, desiredTimestamp, Some(lastSeen))
        .value
        .futureValueUS shouldBe Right(
        Some(
          balance
        )
      )
    }

    "log a warning if returning a balance when no lastSeen was provided and the desired timestamp is after the last update" in {
      val manager = mkManager
      manager.addTrafficBalance(balance).futureValue

      val desiredTimestamp = timestamp.plusSeconds(5)

      val balanceF = loggerFactory.assertLogs(
        manager.getTrafficBalanceAt(member, desiredTimestamp).value,
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
      manager.addTrafficBalance(balance).futureValue

      val desiredTimestamp = timestamp.plusSeconds(5)
      val lastSeen = timestamp.plusSeconds(1)

      // We should be waiting, so make sure the future does not complete
      val resultF = manager.getTrafficBalanceAt(member, desiredTimestamp, Some(lastSeen)).value
      always() {
        resultF.isCompleted shouldBe false
      }

      // Update the balance just before lastSeen
      val balance2 = mkBalance(2, 100, lastSeen.immediatePredecessor)
      manager.addTrafficBalance(balance2).futureValue
      manager.tick(lastSeen)

      // balance2 just became active before last seen, so it should be the one returned
      resultF.futureValueUS shouldBe Right(Some(balance2))
    }

    "auto prune" in {
      val manager = mkManager
      val t0 = clock.now

      // Add a balance so it's in the in memory balance map
      val balance = mkBalance(1, 100, t0)
      val balance2 = mkBalance(2, 200, t0.plusSeconds(1))
      val balance3 = mkBalance(3, 300, t0.plusSeconds(3))

      manager.addTrafficBalance(balance).futureValue
      manager.addTrafficBalance(balance2).futureValue
      manager.addTrafficBalance(balance3).futureValue

      // Set the clock at t0 + 1, this way, the first pruning will run at t0 + 3 (pruning window is 2s),
      // which means everything < t0 + 1 will be prunable
      clock.advanceTo(t0.plusSeconds(1))
      val pruningF = manager.startAutoPruning
      // Advance to t0 + 3 - This should make only the balance at t0 prunable
      // Set safe to prune at t1, to make t0 prunable
      manager.setSafeToPruneBeforeExclusive(t0.plusSeconds(1))
      clock.advanceTo(t0.plusSeconds(3)) // t0 + 3

      // Run an update to trigger pruning
      val balance4 = mkBalance(4, 400, t0.plusSeconds(5))
      manager.addTrafficBalance(balance4).futureValue
      manager.tick(t0.plusSeconds(5).immediateSuccessor)

      eventually() {
        assertFailed(manager, t0.immediateSuccessor)
        assertBalance(manager, t0.plusSeconds(1).immediateSuccessor, balance2)
        assertBalance(manager, t0.plusSeconds(3).immediateSuccessor, balance3)
        assertBalance(manager, t0.plusSeconds(5).immediateSuccessor, balance4)
      }

      // Everything is prunable, but safe to prune is at t0 + 4, so t0 + 3 and t0 + 5 should stay
      // t0 + 3 stays because it's the last balance before t0 + 4. We want to keep it because if we get asked about
      // t0 + 4.5, which is after the pruning threshold, we need to answer with t0 + 3
      manager.setSafeToPruneBeforeExclusive(t0.plusSeconds(4))
      clock.advanceTo(t0.plusSeconds(8)) // t0 + 8
      // Force a prune by adding a balance
      val balance5 = mkBalance(5, 500, t0.plusSeconds(20))
      manager.addTrafficBalance(balance5).futureValue

      eventually() {
        assertFailed(manager, t0.immediateSuccessor)
        assertFailed(manager, t0.plusSeconds(1).immediateSuccessor)
        assertBalance(manager, t0.plusSeconds(3).immediateSuccessor, balance3)
        assertBalance(manager, t0.plusSeconds(4).immediateSuccessor, balance3)
        assertBalance(manager, t0.plusSeconds(5).immediateSuccessor, balance4)
      }

      val closeF = Future(manager.close())
      eventually() {
        closeF.isCompleted shouldBe true
        pruningF.isCompleted shouldBe true
      }
    }

    "read and writes can happen concurrently" in {
      val updatesNb = 200
      val now = clock.now
      val updates = (0 to updatesNb).map(i => mkBalance(i + 1, i.toLong, now.plusSeconds(i.toLong)))
      val manager = mkManager

      // Write a bunch of updates - sequentially because otherwise we could have updates be skipped because serials
      // arrive out of order. But don't wait on the future here
      val writeF = MonadUtil.sequentialTraverse_(updates.toList)(manager.addTrafficBalance)

      // In the meantime, start reading the updates concurrently
      val reads = updates.toList.parTraverse { u =>
        manager.getTrafficBalanceAt(
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
