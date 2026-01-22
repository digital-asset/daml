// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.transports.ServerSubscriptionCloseReason
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencing.service.SubscriptionPool.{
  PoolClosed,
  RegistrationError,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.{FutureUtil, Mutex}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object SubscriptionPool {
  sealed trait RegistrationError
  object PoolClosed extends RegistrationError
}

/** Connects GRPC Sequencer subscriptions with subscriptions to the sequencer itself. Ensures that
  * when one side is closed that the other is. When the subscription manager is closed all active
  * subscriptions and responses will be closed.
  */
class SubscriptionPool[Subscription <: ManagedSubscription](
    clock: Clock,
    metrics: SequencerMetrics,
    maxSubscriptionsPerMember: PositiveInt,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  // as the subscriptions are mutable, any access or modifications to this pool are expected to be synchronized
  private val pool = TrieMap[Member, Vector[Subscription]]()
  private val lock = new Mutex()
  private val subscribersGauge = metrics.publicApi.subscriptionsGauge
  subscribersGauge.updateValue(0)

  def activeSubscriptions(): Seq[Subscription] = pool.values.flatten.toSeq

  /** Create a subscription and register in the pool.
    * @param createSubscription
    *   Function for creating a subscription. Only called if the pool can register new
    *   subscriptions.
    * @return
    *   An error or the subscription we've created.
    */
  def create(
      createSubscription: () => FutureUnlessShutdown[Subscription],
      member: Member,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, RegistrationError, Subscription] =
    synchronizeWithClosing(functionFullName) {
      logger.debug(s"Creating subscription for $member")
      EitherT.right[RegistrationError](createSubscription().map { subscription =>
        {
          // if the subscription is already closed we won't add to the pool in the first place
          if (!(subscription.isClosing || subscription.isCancelled)) {
            val drop = lock.exclusive {
              val (keep, drop) = pool.get(member) match {
                // new subscription for member
                case None =>
                  subscribersGauge.updateValue(_ + 1)
                  (Vector(subscription), Vector.empty[Subscription])
                // member already has subscriptions
                case Some(current) =>
                  val (keep, drop) = current.splitAt(maxSubscriptionsPerMember.value - 1)
                  (subscription +: keep, drop)
              }
              pool.update(member, keep)
              drop
            }
            logger.info(
              s"Adding subscription for $member to pool with ${subscribersGauge.getValue} active"
            )
            if (drop.nonEmpty) {
              logger.info(s"Dropping ${drop.size} excess subscriptions for $member")
              FutureUtil.doNotAwait(
                Future.sequence(
                  drop.map(
                    shutdownSubscription(
                      member,
                      _,
                      transientReason = Some(ServerSubscriptionCloseReason.TooManySubscriptions),
                    )
                  )
                ),
                s"dropping excess subscriptions for $member",
              )
            }
          } else {
            if (subscription.isCancelled) {
              logger.debug(
                s"Not adding subscription for $member to the pool as the request has been cancelled"
              )
              if (!subscription.isClosing) {
                subscription.close()
              }
            } else {
              logger.debug(
                s"Not adding subscription for $member to the pool as it already closing"
              )
            }
          }

          subscription.onClosed(() => removeSubscription(member, subscription).discard)

          // schedule expiring the subscription
          // this could potentially happen immediately if the expiration has already passed
          subscription.expireAt.foreach(ts =>
            clock.scheduleAt(
              _ => {
                // if the subscription still exists, it will be returned here
                removeSubscription(member, subscription).foreach { subscription =>
                  logger.debug(s"Expiring subscription for $member")
                  FutureUtil.doNotAwait(
                    shutdownSubscription(
                      member,
                      subscription,
                      transientReason = Some(ServerSubscriptionCloseReason.TokenExpired),
                    ),
                    s"expiring subscriptions for $member",
                  )
                }
              },
              ts,
            )
          )
        }
        subscription
      })
    } onShutdown Left(PoolClosed)

  def closeSubscriptions(member: Member, waitForClosed: Boolean = false)(implicit
      traceContext: TraceContext
  ): Unit = {
    val subscriptions = lock.exclusive {
      pool.remove(member) match {
        case None => List.empty[Subscription]
        case Some(current) =>
          subscribersGauge.updateValue(_ - 1)
          current.toList
      }
    }
    logger.debug(s"Closing ${subscriptions.size} subscriptions for $member")
    trackShutdown(
      subscriptions.map(sub => (member, shutdownSubscription(member, sub))),
      waitForClosed,
    )
  }

  def closeAllSubscriptions(waitForClosed: Boolean = false)(implicit
      traceContext: TraceContext
  ): Unit = {
    val allSubscriptions = lock.exclusive {
      val all = pool.toList.flatMap { case (member, subscriptions) =>
        subscriptions.map((member, _))
      }
      pool.clear()
      subscribersGauge.updateValue(0)
      all
    }
    trackShutdown(
      allSubscriptions.map { case (member, subscription) =>
        (member, shutdownSubscription(member, subscription))
      },
      waitForClosed,
    )
  }

  private def trackShutdown(items: List[(Member, Future[Unit])], waitForClosed: Boolean)(implicit
      traceContext: TraceContext
  ): Unit =
    items.foreach { case (member, doneF) =>
      val str = s"closing subscription for $member"
      if (waitForClosed) {
        timeouts.unbounded.await(str)(doneF)
      } else {
        FutureUtil.doNotAwait(doneF, str)
      }
    }

  private def shutdownSubscription(
      member: Member,
      subscription: Subscription,
      transientReason: Option[ServerSubscriptionCloseReason.TransientCloseReason] = None,
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.delegate {
      logger.debug(s"Closing subscription for $member")
      try {
        transientReason.fold(subscription.close())(reason => subscription.transientClose(reason))
        subscription.closedF
      } catch {
        // We don't want to throw if closing fails because it will stop the chain of closing subsequent subscriptions
        case NonFatal(e) =>
          logger.warn(s"Failed to close subscription for $member", e)
          Future.unit
      }
    }

  /** Removes the given subscription from the pool
    *
    * @return
    *   the subscription if it was still present (and needs to be closed)
    */
  private def removeSubscription(member: Member, subscription: Subscription)(implicit
      traceContext: TraceContext
  ): Option[Subscription] = {
    val isRemoved = lock.exclusive {
      pool.get(member) match {
        case Some(subscriptions) =>
          val (removed, remaining) = subscriptions.partition(_ == subscription)
          if (remaining.isEmpty) {
            subscribersGauge.updateValue(_ - 1)
            pool.remove(member).discard
          } else {
            pool.update(member, remaining).discard
          }
          removed.nonEmpty
        case None =>
          false
      }
    }
    Option.when(isRemoved) {
      logger.info(s"Removed subscription of $member, now active=${subscribersGauge.getValue}")
      subscription
    }
  }

  override def onClosed(): Unit =
    withNewTraceContext("close_subscription_pool") { implicit traceContext =>
      logger.debug(
        s"Closing all subscriptions in pool with ${subscribersGauge.getValue} subscriptions"
      )
      // wait for the subscriptions to actually close in case they are already in the process of closing
      // in which case FlagClosable doesn't wait.
      closeAllSubscriptions(waitForClosed = true)
    }

}
