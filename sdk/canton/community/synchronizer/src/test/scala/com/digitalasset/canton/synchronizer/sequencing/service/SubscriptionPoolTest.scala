// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.implicits.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{Member, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class SubscriptionPoolTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  def pid(s: String): Member = {
    val id = UniqueIdentifier.fromProtoPrimitive_(s"$s::default").map(ParticipantId(_)).value
    id
  }

  case class Env(
      clock: SimClock,
      pool: SubscriptionPool[MockSubscription],
  )

  object Env {
    def apply(): Env = {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val pool = new SubscriptionPool[MockSubscription](
        clock,
        SequencerTestMetrics,
        timeouts,
        loggerFactory,
      )

      Env(clock, pool)
    }
  }

  class MockSubscription(
      name: String,
      immediatelyClose: Boolean = false,
      isRequestCancelled: Boolean = false,
      val expireAt: Option[CantonTimestamp] = None,
  ) extends ManagedSubscription {
    protected val logger = SubscriptionPoolTest.this.logger

    override protected def timeouts: ProcessingTimeout = SubscriptionPoolTest.this.timeouts

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var closeCount = 0

    if (immediatelyClose) close()

    override def onClosed(): Unit = {
      closeCount += 1
      notifyClosed()
    }

    override def toString: String = s"Subscription($name)"

    override def isCancelled: Boolean = isRequestCancelled
  }

  def createSubscription(name: String): FutureUnlessShutdown[MockSubscription] =
    FutureUnlessShutdown.pure(new MockSubscription(name))

  "SubscriptionPool" should {
    "only close subscriptions that have not already been closed" in {
      val Env(_, manager) = Env()

      // create all in order
      val subscription1 =
        manager.create(() => createSubscription("subscription1"), pid("p1")).futureValue
      val subscription2 =
        manager.create(() => createSubscription("subscription2"), pid("p2")).futureValue
      val subscription3 =
        manager.create(() => createSubscription("subscription3"), pid("p3")).futureValue

      manager.activeSubscriptions() should contain.only(subscription1, subscription2, subscription3)

      // subscription for p2 is going to close itself (perhaps downstream closed their connection)
      subscription2.close()

      eventually() {
        manager.activeSubscriptions() should contain.only(subscription1, subscription3)
      }

      // now we'll close the manager
      manager.close()

      manager.activeSubscriptions() shouldBe empty

      // all should have only been closed once
      subscription1.closeCount shouldEqual 1
      subscription2.closeCount shouldEqual 1
      subscription3.closeCount shouldEqual 1
    }

    "close subscription by member" in {
      val Env(_, manager) = Env()
      val subscription1 =
        manager.create(() => createSubscription("subscription1"), pid("p1")).futureValue
      val subscription2 =
        manager.create(() => createSubscription("subscription2"), pid("p2")).futureValue

      manager.activeSubscriptions() should contain.only(subscription1, subscription2)

      manager.closeSubscriptions(pid("p2"))

      manager.activeSubscriptions() should contain only subscription1
    }

    "closes expired subscriptions" in {
      val Env(clock, manager) = Env()

      val ts1 = CantonTimestamp.Epoch.plusSeconds(5)
      val ts2 = CantonTimestamp.Epoch.plusSeconds(10)

      val subscription1 = manager
        .create(
          () =>
            FutureUnlessShutdown.pure(new MockSubscription("subscription1", expireAt = Some(ts1))),
          pid("p1"),
        )
        .futureValue
      val subscription2 = manager
        .create(
          () =>
            FutureUnlessShutdown.pure(new MockSubscription("subscription2", expireAt = Some(ts2))),
          pid("p1"),
        )
        .futureValue

      manager.activeSubscriptions() should contain.only(subscription1, subscription2)

      clock.advanceTo(ts1)
      manager.activeSubscriptions() should contain only subscription2

      clock.advanceTo(ts2)
      manager.activeSubscriptions() shouldBe empty
    }

    "return error if already shutdown" in {
      val Env(_, manager) = Env()
      manager.close()

      manager
        .create(() => createSubscription("closed"), pid("p1"))
        .value
        .futureValue
        .left
        .value shouldBe SubscriptionPool.PoolClosed
    }

    "immediately closed connection isn't added to pool" in {
      val Env(_, manager) = Env()

      manager
        .create(
          () => FutureUnlessShutdown.pure(new MockSubscription("closed", immediatelyClose = true)),
          pid("p1"),
        )
        .futureValue

      manager.activeSubscriptions() shouldBe empty
    }

    "cancelled request doesn't add connection to pool and doesn't return response" in {
      val Env(_, manager) = Env()

      val subscriptionWithCancelledRequest = new MockSubscription(
        "cancelled",
        isRequestCancelled = true,
      )

      manager
        .create(
          () => FutureUnlessShutdown.pure(subscriptionWithCancelledRequest),
          pid("p1"),
        )
        .futureValue

      // we expect the subscription to be closed immediately
      subscriptionWithCancelledRequest.closeCount shouldEqual 1

      manager.activeSubscriptions() shouldBe empty
    }

    "close subscriptions for member closes all of their subscriptions" in {
      val Env(_, manager) = Env()

      val participant1 = pid("p1")
      val sub1 = new MockSubscription("sub1")
      val sub2 = new MockSubscription("sub2")

      for {
        _ <- List(
          manager.create(() => FutureUnlessShutdown.pure(sub1), participant1),
          manager.create(() => FutureUnlessShutdown.pure(sub2), participant1),
        ).sequence.value.futureValue
        _ = manager.closeSubscriptions(participant1, waitForClosed = true)
      } yield {
        sub1.isClosing shouldBe true
        sub2.isClosing shouldBe true
      }
    }

    "prevent deadlocks on many concurrent subscriptions and create being slow" in {
      val Env(_, manager) = Env()
      val times = 50
      val sleepInterval = 1000L
      val startTime = System.currentTimeMillis()
      (1 to times).toList.parTraverse { i =>
        val sub = new MockSubscription(s"sub-$i")
        val member = pid(s"p$i")
        manager.create(
          () => {
            FutureUnlessShutdown.outcomeF(
              Future {
                Threading.sleep(sleepInterval) // Simulate a slow creation
                sub
              }
            )
          },
          member,
        )
      }.futureValue
      val endTime = System.currentTimeMillis()
      val elapsedTime = endTime - startTime
      logger.debug(s"Elapsed time for $times subscriptions: $elapsedTime ms")
      // Note: we expect subscriptions to be created in parallel,
      // so the total time should close to sleepInterval
      // with 5s generous slack for scheduling delays
      elapsedTime shouldBe <(sleepInterval + 5000L)
    }
  }
}
