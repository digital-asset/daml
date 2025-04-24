// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class SequencerSubscriptionPoolTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ConnectionPoolTestHelpers {
  "SequencerSubscriptionPool" should {
    "initialize in the happy path" in {
      val nbConnections = PositiveInt.tryCreate(10)
      val trustThreshold = PositiveInt.tryCreate(5)
      val livenessMargin = NonNegativeInt.tryCreate(2)

      withConnectionAndSubscriptionPools(
        nbConnections = nbConnections,
        trustThreshold = trustThreshold,
        index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
        livenessMargin = livenessMargin,
      ) { (connectionPool, subscriptionPool, listener) =>
        connectionPool.start()
        subscriptionPool.start()

        clue("normal start") {
          listener.shouldStabilizeOn(ComponentHealthState.Ok())
          subscriptionPool.nbSubscriptions shouldBe (trustThreshold + livenessMargin).toNonNegative
        }
        val initialPool = subscriptionPool.subscriptions.toSeq

        clue("Fail connections") {
          initialPool.foreach(_.connection.fail(reason = "test"))

          // Connections eventually come back
          eventually() {
            subscriptionPool.nbSubscriptions shouldBe (trustThreshold + livenessMargin).toNonNegative
          }
        }
      }
    }

    "handle configuration changes" in {
      val nbConnections = PositiveInt.tryCreate(10)
      val trustThreshold = PositiveInt.tryCreate(5)
      val livenessMargin = NonNegativeInt.tryCreate(3)

      withConnectionAndSubscriptionPools(
        nbConnections = nbConnections,
        trustThreshold = trustThreshold,
        index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
        livenessMargin = livenessMargin,
      ) { (connectionPool, subscriptionPool, listener) =>
        connectionPool.start()
        subscriptionPool.start()

        listener.shouldStabilizeOn(ComponentHealthState.Ok())
        subscriptionPool.nbSubscriptions shouldBe (trustThreshold + livenessMargin).toNonNegative

        clue("Increase liveness margin") {
          val newLivenessMargin = NonNegativeInt.tryCreate(4)
          val newConfig =
            subscriptionPool.config.copy(livenessMargin = newLivenessMargin)
          subscriptionPool.updateConfig(newConfig)

          eventually() {
            subscriptionPool.nbSubscriptions shouldBe (trustThreshold + newLivenessMargin).toNonNegative
          }
        }

        clue("Decrease liveness margin") {
          val newLivenessMargin = NonNegativeInt.tryCreate(1)
          val newConfig =
            subscriptionPool.config.copy(livenessMargin = newLivenessMargin)
          subscriptionPool.updateConfig(newConfig)

          // eventuallyForever() to check we don't remove more than necessary
          eventuallyForever() {
            subscriptionPool.nbSubscriptions shouldBe (trustThreshold + newLivenessMargin).toNonNegative
          }
        }
      }
    }

    "request connections as needed" in {
      val nbConnections = PositiveInt.tryCreate(10)
      val trustThreshold = PositiveInt.tryCreate(5)
      val livenessMargin = NonNegativeInt.tryCreate(2)

      withConnectionAndSubscriptionPools(
        nbConnections = nbConnections,
        trustThreshold = trustThreshold,
        index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
        livenessMargin = livenessMargin,
      ) { (connectionPool, subscriptionPool, listener) =>
        connectionPool.start()
        subscriptionPool.start()

        clue("normal start") {
          listener.shouldStabilizeOn(ComponentHealthState.Ok())
          subscriptionPool.nbSubscriptions shouldBe (trustThreshold + livenessMargin).toNonNegative
        }
        val initialPool = subscriptionPool.subscriptions.toSeq
        val initialConnections = initialPool.map(_.connection.name).toSet

        clue("Gradually remove 3 active connections") {
          initialPool.take(3).zipWithIndex.foreach { case (subscription, index) =>
            subscription.connection.fatal(reason = "test")
            eventually() {
              subscriptionPool.nbSubscriptions shouldBe (trustThreshold + livenessMargin).toNonNegative
              // New connections have been obtained
              val currentConnections = subscriptionPool.subscriptions.map(_.connection.name)
              val delta = initialConnections.diff(currentConnections)
              // The newly obtained connections are different
              delta should have size (index.toLong + 1)
            }
          }
        }

        clue("Remove 1 more to go below the active threshold") {
          initialPool(3).connection.fatal(reason = "test")

          listener.shouldStabilizeOn(
            ComponentHealthState.degraded(
              "below liveness margin: 6 subscription(s) available, trust threshold = 5, liveness margin = 2"
            )
          )
          subscriptionPool.nbSubscriptions shouldBe NonNegativeInt.tryCreate(6)
        }

        clue("Remove 2 more to go below the trust threshold") {
          initialPool.slice(4, 6).foreach(_.connection.fatal(reason = "test"))

          listener.shouldStabilizeOn(
            ComponentHealthState.failed(
              "only 4 subscription(s) available, trust threshold = 5"
            )
          )
          subscriptionPool.nbSubscriptions shouldBe NonNegativeInt.tryCreate(4)
        }
      }
    }
  }
}
