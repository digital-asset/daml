// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.service.SubscriptionPool.{
  PoolClosed,
  RegistrationError,
}
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, blocking}
import scala.util.control.NonFatal

object SubscriptionPool {
  sealed trait RegistrationError
  object PoolClosed extends RegistrationError
}

/** Connects GRPC Sequencer subscriptions with subscriptions to the sequencer itself.
  * Ensures that when one side is closed that the other is.
  * When the subscription manager is closed all active subscriptions and responses will be closed.
  */
class SubscriptionPool[Subscription <: ManagedSubscription](
    clock: Clock,
    metrics: SequencerMetrics,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  // as the subscriptions are mutable, any access or modifications to this pool are expected to be synchronized
  private val pool = TrieMap[Member, mutable.Buffer[Subscription]]()

  private val subscribersGauge = metrics.subscriptionsGauge

  def activeSubscriptions(): Seq[Subscription] = pool.values.flatten.toSeq

  /** Create a subscription and register in the pool.
    * @param createSubscription Function for creating a subscription. Only called if the pool can register new subscriptions.
    * @return An error or the subscription we've created.
    */
  def create(createSubscription: () => Subscription, member: Member)(implicit
      traceContext: TraceContext
  ): Either[RegistrationError, Subscription] =
    performUnlessClosing(functionFullName) {
      blocking {
        synchronized {
          logger.debug(s"Creating subscription for $member")
          val subscription = createSubscription()

          // if the subscription is already closed we won't add to the pool in the first place
          if (!subscription.isClosing) {
            logger.debug(s"Adding subscription from $member to pool")
            pool.update(
              member,
              pool.get(member).fold(mutable.Buffer(subscription)) {
                _ += subscription
              },
            )
          }

          subscription.onClosed(() => removeSubscription(member, subscription))

          // schedule expiring the subscription
          // this could potentially happen immediately if the expiration has already passed
          subscription.expireAt.foreach(ts =>
            clock.scheduleAt(
              _ => {
                logger.debug(s"Expiring subscription for $member")
                closeSubscription(member, subscription)
              },
              ts,
            )
          )

          updatePoolMetrics()

          Right(subscription)
        }
      }
    } onShutdown Left(PoolClosed)

  def closeSubscriptions(member: Member, waitForClosed: Boolean = false)(implicit
      traceContext: TraceContext
  ): Unit =
    blocking {
      synchronized {
        val memberSubscriptions = pool.get(member).map(_.toList).getOrElse(List.empty)
        logger.debug(s"Closing ${memberSubscriptions.size} subscriptions for $member")
        memberSubscriptions.foreach(closeSubscription(member, _, waitForClosed))
      }
    }

  def closeAllSubscriptions(waitForClosed: Boolean = false)(implicit
      traceContext: TraceContext
  ): Unit = {
    pool.foreach { case (member, _) =>
      // safe to modify the underlying collection from the foreach iterator.
      closeSubscriptions(member, waitForClosed)
    }
  }

  private def closeSubscription(
      member: Member,
      subscription: Subscription,
      waitForClosed: Boolean = false,
  )(implicit traceContext: TraceContext): Unit =
    blocking {
      synchronized {
        try {
          logger.debug(s"Closing subscription for $member")
          subscription.close()
          if (waitForClosed)
            timeouts.unbounded.await(s"closing subscription for $member")(subscription.closedF)
        } catch {
          // We don't want to throw if closing fails because it will stop the chain of closing subsequent subscriptions
          case NonFatal(e) => logger.warn(s"Failed to close subscription for $member", e)
        } finally {
          removeSubscription(member, subscription)
        }
      }
    }

  private def removeSubscription(member: Member, subscription: Subscription)(implicit
      traceContext: TraceContext
  ): Unit = blocking {
    synchronized {
      logger.debug(s"Removing subscription for $member")
      val updatedSubscriptions = pool
        .get(member)
        .map(_.filterNot(_ == subscription)) // remove the given subscription
        .filterNot(
          _.isEmpty
        ) // if there are no more subscriptions for the member just return to None

      // if there's no more subscriptions remove the member from the pool (this won't error if they weren't there in the first place)
      // otherwise update the subscriptions buffer
      updatedSubscriptions
        .fold(pool -= member) { subscriptions =>
          pool += member -> subscriptions
        }
        .discard
      updatePoolMetrics()
    }
  }

  override def onClosed(): Unit = blocking {
    synchronized {
      withNewTraceContext { implicit traceContext =>
        logger.debug(s"Closing all subscriptions in pool: $poolDescription")
        // wait for the subscriptions to actually close in case they are already in the process of closing
        // in which case FlagClosable doesn't wait.
        closeAllSubscriptions(waitForClosed = true)
      }
    }
  }

  private def updatePoolMetrics(): Unit = subscribersGauge.updateValue(pool.size)

  private def poolDescription: String =
    pool
      .map { case (member, subs) =>
        s"$member has ${subs.size} subscriptions"
      }
      .mkString(", ")
}
