// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Hash, HashOps, HashPurpose}
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
  SequencerSubscriptionFactoryPekko,
}
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.{DomainId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.OrderedBucketMergeHub.{
  ActiveSourceTerminated,
  ControlOutput,
  DeadlockDetected,
  DeadlockTrigger,
  NewConfiguration,
  OutputElement,
}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{
  OrderedBucketMergeConfig,
  OrderedBucketMergeHub,
  OrderedBucketMergeHubOps,
}
import com.digitalasset.canton.version.RepresentativeProtocolVersion
import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.stream.{KillSwitch, OverflowStrategy}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Aggregates sequenced events from a dynamically configurable set of
  * [[com.digitalasset.canton.sequencing.client.SequencerSubscriptionPekko]]s
  * until a configurable threshold is reached.
  *
  * @param eventValidator The validator used to validate the sequenced events of the
  *                       [[com.digitalasset.canton.sequencing.client.SequencerSubscriptionPekko]]s
  * @param bufferSize How many elements to buffer for each
  *                   [[com.digitalasset.canton.sequencing.client.SequencerSubscriptionPekko]].
  */
class SequencerAggregatorPekko(
    domainId: DomainId,
    eventValidator: SequencedEventValidator,
    bufferSize: PositiveInt,
    hashOps: HashOps,
    override protected val loggerFactory: NamedLoggerFactory,
    enableInvariantCheck: Boolean,
) extends NamedLogging {

  import SequencerAggregatorPekko.*

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
    OrderedBucketMergeConfig[SequencerId, HasSequencerSubscriptionFactoryPekko[E]],
    Either[SubscriptionControl[E], OrdinarySerializedEvent],
    (Future[Done], HealthComponent),
  ] = {
    val onShutdownRunner = new OnShutdownRunner.PureOnShutdownRunner(logger)
    val health = new SequencerAggregatorHealth(domainId, onShutdownRunner, logger)
    val ops = new SequencerAggregatorMergeOps(initialCounterOrPriorEvent, health)
    val hub = new OrderedBucketMergeHub[
      SequencerId,
      OrdinarySerializedEvent,
      HasSequencerSubscriptionFactoryPekko[E],
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
          Left(
            control.map(
              (_, configAndHealth) => configAndHealth._1,
              (_, event) => event,
              Predef.identity,
            )
          )
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

    // By the definition of `Bucket`, the contents
    // and the representative protocol version are the same
    // The SequencedEventValidator ensures that the timestamp of signing key is always None.
    val content = someElem.signedEvent.content
    val representativeProtocolVersion = someElem.signedEvent.representativeProtocolVersion

    // We don't want to force trace contexts to be propagated identically.
    // So lets merge them.
    implicit val mergedTraceContext: TraceContext = TraceContext.ofBatch(elems.values)(logger)

    val mergedSigs = elems.flatMap { case (_, event) => event.signedEvent.signatures }.toSeq
    val mergedSignedEvent = SignedContent
      .create(
        content,
        mergedSigs,
        timestampOfSigningKey = None,
        representativeProtocolVersion,
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
      case DeadlockDetected(elem, trigger) =>
        trigger match {
          case DeadlockTrigger.ActiveSourceTermination =>
            logger.error(
              s"Sequencer subscription for domain $domainId is now stuck. Needs operator intervention to reconfigure the sequencer connections."
            )
          case DeadlockTrigger.Reconfiguration =>
            logger.error(
              s"Reconfiguration of sequencer subscriptions for domain $domainId brings the sequencer subscription to a halt. Needs another reconfiguration."
            )
          case DeadlockTrigger.ElementBucketing =>
            logger.error(
              show"Sequencer subscriptions have diverged and cannot reach the threshold for domain $domainId any more.\nReceived sequenced events: ${elem}"
            )
        }
    }

  private class SequencerAggregatorMergeOps[E: Pretty](
      initialCounterOrPriorEvent: Either[SequencerCounter, PossiblyIgnoredSerializedEvent],
      health: SequencerAggregatorHealth,
  )(implicit val traceContext: TraceContext)
      extends OrderedBucketMergeHubOps[
        SequencerId,
        OrdinarySerializedEvent,
        HasSequencerSubscriptionFactoryPekko[E],
        SequencerCounter,
        HealthComponent,
      ] {

    override type Bucket = SequencerAggregatorPekko.Bucket

    override def prettyBucket: Pretty[Bucket] = implicitly[Pretty[Bucket]]

    override def bucketOf(event: OrdinarySerializedEvent): Bucket =
      Bucket(
        event.counter,
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

    /** The predecessor of the counter to start from */
    override def exclusiveLowerBoundForBegin: SequencerCounter = {
      val counterToSubscribeFrom = initialCounterOrPriorEvent match {
        case Left(initial) => initial
        case Right(priorEvent) =>
          // The client requests the prior event again to check against ledger forks
          priorEvent.counter
      }
      // Subtract 1 to make it exclusive
      counterToSubscribeFrom - 1L
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
        config: HasSequencerSubscriptionFactoryPekko[E],
        exclusiveStart: SequencerCounter,
        priorElement: Option[PriorElement],
    ): Source[OrdinarySerializedEvent, (KillSwitch, Future[Done], HealthComponent)] = {
      val prior = priorElement.collect { case event @ OrdinarySequencedEvent(_, _) => event }
      val subscription = eventValidator
        .validatePekko(config.subscriptionFactory.create(exclusiveStart + 1L), prior, sequencerId)
      val source = subscription.source
        .buffer(bufferSize.value, OverflowStrategy.backpressure)
        .mapConcat(_.value match {
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

object SequencerAggregatorPekko {
  type SubscriptionControl[E] = ControlOutput[
    SequencerId,
    HasSequencerSubscriptionFactoryPekko[E],
    OrdinarySerializedEvent,
    SequencerCounter,
  ]

  private type SubscriptionControlInternal[E] = ControlOutput[
    SequencerId,
    (HasSequencerSubscriptionFactoryPekko[E], Option[HealthComponent]),
    OrdinarySerializedEvent,
    SequencerCounter,
  ]

  trait HasSequencerSubscriptionFactoryPekko[E] {
    def subscriptionFactory: SequencerSubscriptionFactoryPekko[E]
  }

  private[SequencerAggregatorPekko] final case class Bucket(
      sequencerCounter: SequencerCounter,
      contentHash: Hash,
      representativeProtocolVersion: RepresentativeProtocolVersion[SignedContent.type],
  ) extends PrettyPrinting {
    override def pretty: Pretty[Bucket] =
      prettyOfClass(
        param("sequencer counter", _.sequencerCounter),
        param("content hash", _.contentHash),
      )
  }

  private[SequencerAggregatorPekko] class SequencerAggregatorHealth(
      private val domainId: DomainId,
      override protected val associatedOnShutdownRunner: OnShutdownRunner,
      override protected val logger: TracedLogger,
  ) extends CompositeHealthComponent[SequencerId, HealthComponent]
      with PrettyPrinting {

    private val additionalState: AtomicReference[SequencerAggregatorHealth.State] =
      new AtomicReference[SequencerAggregatorHealth.State](
        SequencerAggregatorHealth.State(PositiveInt.one, deadlocked = false)
      )

    override val name: String = s"sequencer-subscription-$domainId"

    override protected def initialHealthState: ComponentHealthState =
      ComponentHealthState.NotInitializedState

    override def closingState: ComponentHealthState =
      ComponentHealthState.failed(s"Disconnected from domain $domainId")

    override protected def combineDependentStates: ComponentHealthState = {
      val state = additionalState.get()
      if (state.deadlocked) {
        ComponentHealthState.failed(
          s"Sequencer subscriptions have diverged and cannot reach the threshold ${state.currentThreshold} for domain $domainId any more."
        )
      } else {
        SequencerAggregator.aggregateHealthResult(
          getDependencies.fmap(_.getState),
          state.currentThreshold,
        )
      }
    }

    def updateHealth(control: SubscriptionControlInternal[?]): Unit = {
      control match {
        case NewConfiguration(newConfig, _startingOffset) =>
          val currentlyRegisteredDependencies = getDependencies
          val toRemove = currentlyRegisteredDependencies.keySet diff newConfig.sources.keySet
          val toAdd = newConfig.sources.collect { case (id, (_config, Some(health))) =>
            (id, health)
          }
          val newThreshold = newConfig.threshold
          val previousState =
            additionalState.getAndSet(
              SequencerAggregatorHealth.State(newThreshold, deadlocked = false)
            )
          alterDependencies(toRemove, toAdd)
          // Separately trigger a refresh in case no dependencies had changed.
          if (newThreshold != previousState.currentThreshold)
            refreshFromDependencies()(TraceContext.empty)
        case ActiveSourceTerminated(sequencerId, _cause) =>
          alterDependencies(remove = Set(sequencerId), add = Map.empty)
        case DeadlockDetected(_, _) =>
          additionalState.getAndUpdate(_.copy(deadlocked = true))
          refreshFromDependencies()(TraceContext.empty)
      }
    }

    override def pretty: Pretty[SequencerAggregatorHealth] = prettyOfClass(
      param("domain id", _.domainId),
      param("state", _.getState),
    )
  }

  private[SequencerAggregatorPekko] object SequencerAggregatorHealth {
    private final case class State(
        currentThreshold: PositiveInt,
        deadlocked: Boolean,
    )
  }
}
