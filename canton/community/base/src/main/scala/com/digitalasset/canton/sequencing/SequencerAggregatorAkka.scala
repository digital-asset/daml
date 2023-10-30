// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import akka.Done
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.{KillSwitch, OverflowStrategy}
import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Hash, HashOps, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.{
  ComponentHealthState,
  CompositeHealthComponent,
  HealthComponent,
}
import com.digitalasset.canton.lifecycle.OnShutdownRunner
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.client.{
  SequencedEventValidator,
  SequencerSubscriptionFactoryAkka,
}
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.{DomainId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.OrderedBucketMergeHub.{
  ActiveSourceTerminated,
  ControlOutput,
  NewConfiguration,
  OutputElement,
}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{
  ErrorUtil,
  OrderedBucketMergeConfig,
  OrderedBucketMergeHub,
  OrderedBucketMergeHubOps,
}
import com.digitalasset.canton.version.RepresentativeProtocolVersion

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Aggregates sequenced events from a dynamically configurable set of
  * [[com.digitalasset.canton.sequencing.client.SequencerSubscriptionAkka]]s
  * until a configurable threshold is reached.
  *
  * @param eventValidator The validator used to validate the sequenced events of the
  *                       [[com.digitalasset.canton.sequencing.client.SequencerSubscriptionAkka]]s
  * @param bufferSize How many elements to buffer for each
  *                   [[com.digitalasset.canton.sequencing.client.SequencerSubscriptionAkka]].
  */
class SequencerAggregatorAkka(
    domainId: DomainId,
    eventValidator: SequencedEventValidator,
    bufferSize: PositiveInt,
    hashOps: HashOps,
    override protected val loggerFactory: NamedLoggerFactory,
    enableInvariantCheck: Boolean,
) extends NamedLogging {

  import SequencerAggregatorAkka.*

  /** Convert a stream of sequencer configurations into a stream of aggregated sequenced events.
    *
    * Must be materialized at most once.
    *
    * @param initialCounterOrPriorEvent The sequencer counter to start the subscription from or the prior event to validate the subscription against.
    *                                   If present, the prior event's sequencer counter determines the subscription start.
    */
  def aggregateFlow[E: Pretty](
      initialCounterOrPriorEvent: Either[SequencerCounter, PossiblyIgnoredSerializedEvent]
  )(implicit traceContext: TraceContext, executionContext: ExecutionContext): Flow[
    OrderedBucketMergeConfig[SequencerId, HasSequencerSubscriptionFactoryAkka[E]],
    Either[SubscriptionControl[E], OrdinarySerializedEvent],
    (Future[Done], HealthComponent),
  ] = {
    val onShutdownRunner = new OnShutdownRunner.PureOnShutdownRunner(logger)
    val health = new SequencerAggregatorHealth(domainId, onShutdownRunner, logger)
    val ops = new SequencerAggregatorMergeOps(initialCounterOrPriorEvent, health)
    val hub = new OrderedBucketMergeHub[
      SequencerId,
      OrdinarySerializedEvent,
      HasSequencerSubscriptionFactoryAkka[E],
      SequencerCounter,
      HealthComponent,
    ](ops, loggerFactory, enableInvariantCheck)
    Flow
      .fromGraph(hub)
      .map {
        case OutputElement(elems) => Right(mergeBucket(elems))
        case control: SubscriptionControlInternal[E] =>
          logError(control)
          health.updateHealth(control)
          Left(control.map((_, configAndHealth) => configAndHealth._1, Predef.identity))
      }
      .mapMaterializedValue { doneF =>
        val doneAndClosedF = doneF.thereafter { _ => onShutdownRunner.close() }
        doneAndClosedF -> health
      }
  }

  private def mergeBucket(
      elems: NonEmpty[Map[SequencerId, OrdinarySerializedEvent]]
  ): OrdinarySerializedEvent = {
    val (_, someElem) = elems.head1

    // By the definition of `Bucket`, the contents, timestamp of signing key
    // and the representative protocol version are the same
    val content = someElem.signedEvent.content
    val timestampOfSigningKey = someElem.signedEvent.timestampOfSigningKey
    val representativeProtocolVersion = someElem.signedEvent.representativeProtocolVersion

    // We don't want to force trace contexts to be propagated identically.
    // So lets merge them.
    implicit val mergedTraceContext: TraceContext = TraceContext.ofBatch(elems.values)(logger)

    val mergedSigs = elems.flatMap { case (_, event) => event.signedEvent.signatures }.toSeq
    val mergedSignedEvent = SignedContent
      .create(content, mergedSigs, timestampOfSigningKey, representativeProtocolVersion)
      .valueOr(err =>
        ErrorUtil.invalidState(s"Failed to aggregate signatures on sequenced event: $err")
      )
    // TODO(#13789) How should we merge the traffic state as it's currently not part of the bucketing?
    val mergedTrafficState = someElem.trafficState

    // We intentionally do not use the copy method
    // so that we notice when fields are added
    OrdinarySequencedEvent(mergedSignedEvent, mergedTrafficState)(mergedTraceContext)
  }

  private def logError[E: Pretty](
      control: SubscriptionControlInternal[E]
  )(implicit traceContext: TraceContext): Unit =
    control match {
      case ActiveSourceTerminated(sequencerId, cause) =>
        cause.foreach { ex => logger.error(s"Sequencer subscription for $sequencerId failed", ex) }
      case NewConfiguration(_, _) =>
    }

  private class SequencerAggregatorMergeOps[E: Pretty](
      initialCounterOrPriorEvent: Either[SequencerCounter, PossiblyIgnoredSerializedEvent],
      health: SequencerAggregatorHealth,
  )(implicit val traceContext: TraceContext)
      extends OrderedBucketMergeHubOps[
        SequencerId,
        OrdinarySerializedEvent,
        HasSequencerSubscriptionFactoryAkka[E],
        SequencerCounter,
        HealthComponent,
      ] {

    override type Bucket = SequencerAggregatorAkka.Bucket

    override def prettyBucket: Pretty[Bucket] = implicitly[Pretty[Bucket]]

    override def bucketOf(event: OrdinarySerializedEvent): Bucket =
      Bucket(
        event.counter,
        event.signedEvent.timestampOfSigningKey,
        // keep only the content hash instead of the content itself.
        // This will allow us to eventually request only signatures from some sequencers to save bandwidth
        SignedContent.hashContent(
          hashOps,
          event.signedEvent.content,
          HashPurpose.SequencedEventSignature,
        ),
        event.signedEvent.representativeProtocolVersion,
        // TODO(#13789) What do we do about the traffic state?
        //  If the traffic state was covered by the signature, we wouldn't need to worry about this here,
        //  but then the traffic state becomes part of a proof of sequencing and thus needs to be shown to third parties.
        //  Clearly, this can be avoided with a Merkle tree!
      )

    override def orderingOffset: Ordering[SequencerCounter] = Ordering[SequencerCounter]

    override def offsetOfBucket(bucket: Bucket): SequencerCounter = bucket.sequencerCounter

    /** The initial offset to start from */
    override def exclusiveLowerBoundForBegin: SequencerCounter = initialCounterOrPriorEvent match {
      case Left(initial) => initial - 1L
      case Right(priorEvent) => priorEvent.counter
    }

    override def traceContextOf(event: OrdinarySerializedEvent): TraceContext =
      event.traceContext

    override type PriorElement = PossiblyIgnoredSerializedEvent

    override def priorElement: Option[PossiblyIgnoredSerializedEvent] =
      initialCounterOrPriorEvent.toOption

    override def toPriorElement(
        output: OrderedBucketMergeHub.OutputElement[SequencerId, OrdinarySerializedEvent]
    ): PriorElement = mergeBucket(output.elem)

    override def makeSource(
        sequencerId: SequencerId,
        config: HasSequencerSubscriptionFactoryAkka[E],
        exclusiveStart: SequencerCounter,
        priorElement: Option[PriorElement],
    ): Source[OrdinarySerializedEvent, (KillSwitch, Future[Done], HealthComponent)] = {
      val prior = priorElement.collect { case event @ OrdinarySequencedEvent(_, _) => event }
      val subscription = eventValidator
        .validateAkka(config.subscriptionFactory.create(exclusiveStart), prior, sequencerId)
      val source = subscription.source
        .buffer(bufferSize.value, OverflowStrategy.backpressure)
        .mapConcat(_.unwrap match {
          case Left(err) =>
            // Errors cannot be aggregated because they are specific to a particular sequencer or subscription.
            // So we log them here and do not propagate them.
            // Health reporting will pick up the termination of the sequencer connection,
            // but doesn't need to know the reason for the failure.
            logger.warn(s"Sequencer subscription for $sequencerId failed with $err.")
            // Note that we cannot tunnel the error through the aggregation as a thrown exception
            // because a failure would send a cancellation signal up the stream,
            // which we are trying to avoid for clean shutdown reasons.
            None
          case Right(event) => Some(event)
        })
      source.mapMaterializedValue { case (killSwitch, doneF) =>
        (killSwitch, doneF, subscription.health)
      }
    }
  }
}

object SequencerAggregatorAkka {
  type SubscriptionControl[E] = ControlOutput[
    SequencerId,
    HasSequencerSubscriptionFactoryAkka[E],
    SequencerCounter,
  ]

  private type SubscriptionControlInternal[E] = ControlOutput[
    SequencerId,
    (HasSequencerSubscriptionFactoryAkka[E], Option[HealthComponent]),
    SequencerCounter,
  ]

  trait HasSequencerSubscriptionFactoryAkka[E] {
    def subscriptionFactory: SequencerSubscriptionFactoryAkka[E]
  }

  private[SequencerAggregatorAkka] final case class Bucket(
      sequencerCounter: SequencerCounter,
      timestampOfSigningKey: Option[CantonTimestamp],
      contentHash: Hash,
      representativeProtocolVersion: RepresentativeProtocolVersion[SignedContent.type],
  ) extends PrettyPrinting {
    override def pretty: Pretty[Bucket] =
      prettyOfClass(
        param("sequencer counter", _.sequencerCounter),
        paramIfDefined("timestamp of signing key", _.timestampOfSigningKey),
        param("content hash", _.contentHash),
      )
  }

  private[SequencerAggregatorAkka] class SequencerAggregatorHealth(
      private val domainId: DomainId,
      override protected val associatedOnShutdownRunner: OnShutdownRunner,
      override protected val logger: TracedLogger,
  ) extends CompositeHealthComponent[SequencerId, HealthComponent]
      with PrettyPrinting {

    private val currentThreshold = new AtomicReference[PositiveInt](PositiveInt.one)

    override val name: String = s"sequencer-subscription-$domainId"

    override protected def initialHealthState: ComponentHealthState =
      ComponentHealthState.NotInitializedState

    override def closingState: ComponentHealthState =
      ComponentHealthState.failed(s"Disconnected from domain $domainId")

    override protected def combineDependentStates: ComponentHealthState = {
      val threshold = currentThreshold.get
      SequencerAggregator.aggregateHealthResult(getDependencies.fmap(_.getState), threshold)
    }

    def updateHealth(control: SubscriptionControlInternal[?]): Unit = {
      control match {
        case NewConfiguration(newConfig, startingOffset) =>
          val currentlyRegisteredDependencies = getDependencies
          val toRemove = currentlyRegisteredDependencies.keySet diff newConfig.sources.keySet
          val toAdd = newConfig.sources.collect { case (id, (_config, Some(health))) =>
            (id, health)
          }
          val newThreshold = newConfig.threshold
          val previousThreshold = currentThreshold.getAndSet(newThreshold)
          alterDependencies(toRemove, toAdd)
          // Separately trigger a refresh in case no dependencies had changed.
          if (newThreshold != previousThreshold)
            refreshFromDependencies()(TraceContext.empty)
        case ActiveSourceTerminated(sequencerId, _cause) =>
          alterDependencies(remove = Set(sequencerId), add = Map.empty)
      }
    }

    override def pretty: Pretty[SequencerAggregatorHealth] = prettyOfClass(
      param("domain id", _.domainId),
      param("state", _.getState),
    )
  }
}
