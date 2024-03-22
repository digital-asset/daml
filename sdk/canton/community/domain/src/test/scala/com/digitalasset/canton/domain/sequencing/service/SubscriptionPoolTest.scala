// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.implicits.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.DomainTestMetrics
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{Member, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class SubscriptionPoolTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  def pid(s: String): Member = {
    val id = UniqueIdentifier.fromProtoPrimitive_(s"$s::default").map(ParticipantId(_)).value
    id
  }

  case class Env(clock: SimClock, pool: SubscriptionPool[MockSubscription])

  object Env {
    def apply(): Env = {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val pool = new SubscriptionPool[MockSubscription](
        clock,
        DomainTestMetrics.sequencer,
        timeouts,
        loggerFactory,
      )

      Env(clock, pool)
    }
  }

  class MockSubscription(
      name: String,
      immediatelyClose: Boolean = false,
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
  }

  def createSubscription(name: String): MockSubscription = new MockSubscription(name)

  "SubscriptionPool" should {
    "only close subscriptions that have not already been closed" in {
      val Env(_, manager) = Env()

      // create all in order
      val subscription1 = manager.create(() => createSubscription("subscription1"), pid("p1")).value
      val subscription2 = manager.create(() => createSubscription("subscription2"), pid("p2")).value
      val subscription3 = manager.create(() => createSubscription("subscription3"), pid("p3")).value

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
      val subscription1 = manager.create(() => createSubscription("subscription1"), pid("p1")).value
      val subscription2 = manager.create(() => createSubscription("subscription2"), pid("p2")).value

      manager.activeSubscriptions() should contain.only(subscription1, subscription2)

      manager.closeSubscriptions(pid("p2"))

      manager.activeSubscriptions() should contain only subscription1
    }

    "closes expired subscriptions" in {
      val Env(clock, manager) = Env()

      val ts1 = CantonTimestamp.Epoch.plusSeconds(5)
      val ts2 = CantonTimestamp.Epoch.plusSeconds(10)

      val subscription1 = manager
        .create(() => new MockSubscription("subscription1", expireAt = Some(ts1)), pid("p1"))
        .valueOrFail("subscription1")
      val subscription2 = manager
        .create(() => new MockSubscription("subscription2", expireAt = Some(ts2)), pid("p1"))
        .valueOrFail("subscription2")

      manager.activeSubscriptions() should contain.only(subscription1, subscription2)

      clock.advanceTo(ts1)
      manager.activeSubscriptions() should contain only subscription2

      clock.advanceTo(ts2)
      manager.activeSubscriptions() shouldBe empty
    }

    "return error if already shutdown" in {
      val Env(_, manager) = Env()
      manager.close()

      manager.create(() => createSubscription("closed"), pid("p1")).left.value should matchPattern {
        case SubscriptionPool.PoolClosed =>
      }
    }

    "immediately closed connection isn't added to pool" in {
      val Env(_, manager) = Env()

      manager.create(() => new MockSubscription("closed", immediatelyClose = true), pid("p1"))

      manager.activeSubscriptions() shouldBe empty
    }

    "close subscriptions for member closes all of their subscriptions" in {
      val Env(_, manager) = Env()

      val participant1 = pid("p1")
      val sub1 = new MockSubscription("sub1")
      val sub2 = new MockSubscription("sub2")

      for {
        _ <- List(
          manager.create(() => sub1, participant1),
          manager.create(() => sub2, participant1),
        ).sequence
        _ = manager.closeSubscriptions(participant1, waitForClosed = true)
      } yield {
        sub1.isClosing shouldBe true
        sub2.isClosing shouldBe true
      }
    }
  }
}
