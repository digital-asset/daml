// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.Monoid
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.protocol.Envelope
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

/** An application handler processes boxed envelopes and returns a [[HandlerResult]] */
trait ApplicationHandler[-Box[+_ <: Envelope[_]], -Env <: Envelope[_]]
    extends (BoxedEnvelope[Box, Env] => HandlerResult) {

  /** Human-readable name of the application handler for logging and debugging */
  def name: String

  /** Called by the [[com.digitalasset.canton.sequencing.client.SequencerClient]] before the start of a subscription.
    * @param domainTimeTracker The domain time tracker that listens to this application handler's subscription
    */
  def subscriptionStartsAt(start: SubscriptionStart, domainTimeTracker: DomainTimeTracker)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Replaces the application handler's processing with `f` and
    * leaves the [[subscriptionStartsAt]] logic and the name the same.
    */
  def replace[Box2[+_ <: Envelope[_]], Env2 <: Envelope[_]](
      f: BoxedEnvelope[Box2, Env2] => HandlerResult
  ): ApplicationHandler[Box2, Env2] = new ApplicationHandler[Box2, Env2] {

    override def name: String = ApplicationHandler.this.name

    override def subscriptionStartsAt(
        start: SubscriptionStart,
        domainTimeTracker: DomainTimeTracker,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] =
      ApplicationHandler.this.subscriptionStartsAt(start, domainTimeTracker)

    override def apply(boxedEnvelope: BoxedEnvelope[Box2, Env2]): HandlerResult =
      f(boxedEnvelope)
  }

  /** Run the `other` ApplicationHandler after `this`. */
  def combineWith[Box2[+X <: Envelope[_]] <: Box[X], Env2 <: Env](
      other: ApplicationHandler[Box2, Env2]
  )(implicit
      ec: ExecutionContext
  ): ApplicationHandler[Box2, Env2] = new ApplicationHandler[Box2, Env2] {

    override def name: String =
      s"${ApplicationHandler.this.name}+${other.name}"

    override def subscriptionStartsAt(
        start: SubscriptionStart,
        domainTimeTracker: DomainTimeTracker,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
      for {
        _ <- ApplicationHandler.this.subscriptionStartsAt(start, domainTimeTracker)
        _ <- other.subscriptionStartsAt(start, domainTimeTracker)
      } yield ()

    override def apply(boxedEnvelope: BoxedEnvelope[Box2, Env2]): HandlerResult = {
      for {
        r1 <- ApplicationHandler.this.apply(boxedEnvelope: BoxedEnvelope[Box, Env])
        r2 <- other.apply(boxedEnvelope)
      } yield Monoid[AsyncResult].combine(r1, r2)
    }
  }
}

object ApplicationHandler {

  /** Creates an application handler that runs `f` on the boxed envelopes
    * and ignores the [[ApplicationHandler.subscriptionStartsAt]] notifications
    */
  def create[Box[+_ <: Envelope[_]], Env <: Envelope[_]](name: String)(
      f: BoxedEnvelope[Box, Env] => HandlerResult
  ): ApplicationHandler[Box, Env] = {
    val handlerName = name
    new ApplicationHandler[Box, Env] {

      override val name: String = handlerName

      override def subscriptionStartsAt(
          start: SubscriptionStart,
          domainTimeTracker: DomainTimeTracker,
      )(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Unit] =
        FutureUnlessShutdown.unit

      override def apply(boxedEnvelope: BoxedEnvelope[Box, Env]): HandlerResult = f(boxedEnvelope)
    }
  }

  /** Application handler that does nothing and always succeeds */
  @VisibleForTesting
  def success[Box[+_ <: Envelope[_]], Env <: Envelope[_]](
      name: String = "success"
  ): ApplicationHandler[Box, Env] =
    ApplicationHandler.create(name)(_ => HandlerResult.done)
}

/** Information passed by the [[com.digitalasset.canton.sequencing.client.SequencerClient]]
  * to the [[ApplicationHandler]] where the subscription (= processing of events) starts.
  * The [[ApplicationHandler]] can then initialize itself appropriately.
  */
sealed trait SubscriptionStart extends Product with Serializable with PrettyPrinting

/** The subscription is a resubscription. The application handler may have previously been called with an event. */
sealed trait ResubscriptionStart extends SubscriptionStart

object SubscriptionStart {

  /** The subscription is created for the first time.
    * The application handler has never been called with an event.
    */
  case object FreshSubscription extends SubscriptionStart {
    override def pretty: Pretty[FreshSubscription] = prettyOfObject[FreshSubscription]
  }
  type FreshSubscription = FreshSubscription.type

  /** The first processed event is at some timestamp after the `cleanPrehead`.
    * All events up to `cleanPrehead` inclusive have previously been processed completely.
    * The application handler has never been called with an event with a higher timestamp.
    */
  final case class CleanHeadResubscriptionStart(cleanPrehead: CantonTimestamp)
      extends ResubscriptionStart {

    override def pretty: Pretty[CleanHeadResubscriptionStart] = prettyOfClass(
      param("clean prehead", _.cleanPrehead)
    )
  }

  /** The first processed event will be `firstReplayed`.
    *
    * @param cleanPreheadO The timestamp of the last event known to be clean.
    *                      If set, this may be before, at, or after `firstReplayed`.
    *                      If it is before `firstReplayed`,
    *                      then `firstReplayed` is the timestamp of the first event after `cleanPreheadO`.
    */
  final case class ReplayResubscriptionStart(
      firstReplayed: CantonTimestamp,
      cleanPreheadO: Option[CantonTimestamp],
  ) extends ResubscriptionStart {
    override def pretty: Pretty[ReplayResubscriptionStart] = prettyOfClass(
      param("first replayed", _.firstReplayed),
      paramIfDefined("clean prehead", _.cleanPreheadO),
    )
  }

}
