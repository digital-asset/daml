// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Hash, HashOps, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.{
  ComponentHealthState,
  CompositeHealthComponent,
  HealthComponent,
}
import com.digitalasset.canton.lifecycle.{HasRunOnClosing, OnShutdownRunner}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.client.{
  SequencedEventValidator,
  SequencerClient,
  SequencerSubscriptionFactoryPekko,
}
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.topology.{SequencerId, SynchronizerId}
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
  * [[com.digitalasset.canton.sequencing.client.SequencerSubscriptionPekko]]s until a configurable
  * threshold is reached.
  *
  * @param createEventValidator
  *   The validator used to validate the sequenced events of the
  *   [[com.digitalasset.canton.sequencing.client.SequencerSubscriptionPekko]]s
  * @param bufferSize
  *   How many elements to buffer for each
  *   [[com.digitalasset.canton.sequencing.client.SequencerSubscriptionPekko]].
  */
class SequencerAggregatorPekko(
    synchronizerId: SynchronizerId,
    createEventValidator: NamedLoggerFactory => SequencedEventValidator,
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
    * @param initialTimestampOrPriorEvent
    *   The timestamp (or None) to start the subscription from or the prior event to validate the
    *   subscription against. If present, the prior event's sequencing timestamp determines the
    *   subscription start.
    */
  def aggregateFlow[E: Pretty](
      initialTimestampOrPriorEvent: Either[Option[CantonTimestamp], ProcessingSerializedEvent]
  )(implicit traceContext: TraceContext, executionContext: ExecutionContext): Flow[
    OrderedBucketMergeConfig[SequencerId, HasSequencerSubscriptionFactoryPekko[E]],
    Either[SubscriptionControl[E], SequencedSerializedEvent],
    (Future[Done], HealthComponent),
  ] = {
    val onShutdownRunner = new OnShutdownRunner.PureOnShutdownRunner(logger)
    val health = new SequencerAggregatorHealth(synchronizerId, onShutdownRunner, logger)
    val ops = new SequencerAggregatorMergeOps(initialTimestampOrPriorEvent)
    val hub = new OrderedBucketMergeHub[
      SequencerId,
      SequencedSerializedEvent,
      HasSequencerSubscriptionFactoryPekko[E],
      Option[CantonTimestamp],
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
        val doneAndClosedF = doneF.thereafter(_ => onShutdownRunner.close())
        doneAndClosedF -> health
      }
  }

  private def mergeBucket(
      elems: NonEmpty[Map[SequencerId, SequencedSerializedEvent]]
  ): SequencedSerializedEvent = {
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
    // We intentionally do not use the copy method
    // so that we notice when fields are added
    SequencedEventWithTraceContext(mergedSignedEvent)(mergedTraceContext)
  }

  private def logError[E](
      control: SubscriptionControlInternal[E]
  )(implicit traceContext: TraceContext): Unit =
    control match {
      case ActiveSourceTerminated(sequencerId, cause) =>
        cause.foreach(ex => logger.error(s"Sequencer subscription for $sequencerId failed", ex))
      case NewConfiguration(_, _) =>
      case DeadlockDetected(elem, trigger) =>
        trigger match {
          case DeadlockTrigger.ActiveSourceTermination =>
            logger.error(
              s"Sequencer subscription for synchronizer $synchronizerId is now stuck. Needs operator intervention to reconfigure the sequencer connections."
            )
          case DeadlockTrigger.Reconfiguration =>
            logger.error(
              s"Reconfiguration of sequencer subscriptions for synchronizer $synchronizerId brings the sequencer subscription to a halt. Needs another reconfiguration."
            )
          case DeadlockTrigger.ElementBucketing =>
            logger.error(
              show"Sequencer subscriptions have diverged and cannot reach the threshold for synchronizer $synchronizerId any more.\nReceived sequenced events: $elem"
            )
        }
    }

  private class SequencerAggregatorMergeOps[E: Pretty](
      initialTimestampOrPriorEvent: Either[Option[CantonTimestamp], ProcessingSerializedEvent]
  )(implicit val traceContext: TraceContext)
      extends OrderedBucketMergeHubOps[
        SequencerId,
        SequencedSerializedEvent,
        HasSequencerSubscriptionFactoryPekko[E],
        Option[CantonTimestamp],
        HealthComponent,
      ] {

    override type Bucket = SequencerAggregatorPekko.Bucket

    override def prettyBucket: Pretty[Bucket] = implicitly[Pretty[Bucket]]

    override def bucketOf(event: SequencedSerializedEvent): Bucket =
      Bucket(
        Some(event.timestamp),
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

    override def orderingOffset: Ordering[Option[CantonTimestamp]] =
      Ordering[Option[CantonTimestamp]]

    override def offsetOfBucket(bucket: Bucket): Option[CantonTimestamp] =
      bucket.sequencingTimestamp

    /** The predecessor timestamp to start from, should produce predecessor element for verification
      */
    override def initialOffset: Option[CantonTimestamp] = {
      val timestampToSubscribeFrom = initialTimestampOrPriorEvent match {
        case Left(initial) => initial
        case Right(priorEvent) =>
          // The client requests the prior event again to check against ledger forks
          Some(priorEvent.timestamp)
      }
      timestampToSubscribeFrom
    }

    override def traceContextOf(event: SequencedSerializedEvent): TraceContext =
      event.traceContext

    override type PriorElement = ProcessingSerializedEvent

    override def priorElement: Option[ProcessingSerializedEvent] =
      initialTimestampOrPriorEvent.toOption

    override def toPriorElement(
        output: OrderedBucketMergeHub.OutputElement[SequencerId, SequencedSerializedEvent]
    ): PriorElement = mergeBucket(output.elem)

    override def makeSource(
        sequencerId: SequencerId,
        config: HasSequencerSubscriptionFactoryPekko[E],
        startFromInclusive: Option[CantonTimestamp],
        priorElement: Option[PriorElement],
    ): Source[SequencedSerializedEvent, (KillSwitch, Future[Done], HealthComponent)] = {
      val prior = priorElement.collect { case event @ SequencedEventWithTraceContext(_) =>
        event
      }
      val eventValidator = createEventValidator(
        SequencerClient.loggerFactoryWithSequencerId(loggerFactory, sequencerId)
      )
      val subscription = eventValidator
        .validatePekko(config.subscriptionFactory.create(startFromInclusive), prior, sequencerId)
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
    SequencedSerializedEvent,
    Option[CantonTimestamp],
  ]

  private type SubscriptionControlInternal[E] = ControlOutput[
    SequencerId,
    (HasSequencerSubscriptionFactoryPekko[E], Option[HealthComponent]),
    SequencedSerializedEvent,
    Option[CantonTimestamp],
  ]

  trait HasSequencerSubscriptionFactoryPekko[E] {
    def subscriptionFactory: SequencerSubscriptionFactoryPekko[E]
  }

  private[SequencerAggregatorPekko] final case class Bucket(
      sequencingTimestamp: Option[CantonTimestamp],
      contentHash: Hash,
      representativeProtocolVersion: RepresentativeProtocolVersion[SignedContent.type],
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[Bucket] =
      prettyOfClass(
        param("sequencing timestamp", _.sequencingTimestamp),
        param("content hash", _.contentHash),
      )
  }

  private[SequencerAggregatorPekko] class SequencerAggregatorHealth(
      private val synchronizerId: SynchronizerId,
      override protected val associatedHasRunOnClosing: HasRunOnClosing,
      override protected val logger: TracedLogger,
  ) extends CompositeHealthComponent[SequencerId, HealthComponent]
      with PrettyPrinting {

    private val additionalState: AtomicReference[SequencerAggregatorHealth.State] =
      new AtomicReference[SequencerAggregatorHealth.State](
        SequencerAggregatorHealth.State(PositiveInt.one, deadlocked = false)
      )

    override val name: String = s"sequencer-subscription-$synchronizerId"

    override protected def initialHealthState: ComponentHealthState =
      ComponentHealthState.NotInitializedState

    override def closingState: ComponentHealthState =
      ComponentHealthState.failed(s"Disconnected from synchronizer $synchronizerId")

    override protected def combineDependentStates: ComponentHealthState = {
      val state = additionalState.get()
      if (state.deadlocked) {
        ComponentHealthState.failed(
          s"Sequencer subscriptions have diverged and cannot reach the threshold ${state.currentThreshold} for synchronizer $synchronizerId any more."
        )
      } else {
        SequencerAggregator.aggregateHealthResult(
          getDependencies.fmap(_.getState),
          state.currentThreshold,
        )
      }
    }

    def updateHealth(control: SubscriptionControlInternal[?]): Unit =
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

    override protected def pretty: Pretty[SequencerAggregatorHealth] = prettyOfClass(
      param("synchronizer id", _.synchronizerId),
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
