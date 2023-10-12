// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import akka.Done
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.{KillSwitch, OverflowStrategy}
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Hash, HashOps, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.{
  SequencedEventValidator,
  SequencerSubscriptionFactoryAkka,
}
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.OrderedBucketMergeHub.{
  ActiveSourceTerminated,
  ControlOutput,
  NewConfiguration,
  OutputElement,
}
import com.digitalasset.canton.util.{
  ErrorUtil,
  OrderedBucketMergeConfig,
  OrderedBucketMergeHub,
  OrderedBucketMergeHubOps,
}
import com.digitalasset.canton.version.RepresentativeProtocolVersion

import scala.concurrent.Future

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
    eventValidator: SequencedEventValidator,
    bufferSize: PositiveInt,
    hashOps: HashOps,
    override protected val loggerFactory: NamedLoggerFactory,
    enableInvariantCheck: Boolean,
) extends NamedLogging {

  import SequencerAggregatorAkka.*

  /** Create a stream of aggregated sequenced events.
    *
    * @param initialCounterOrPriorEvent The sequencer counter to start the subscription from or the prior event to validate the subscription against.
    *                                   If present, the prior event's sequencer counter determines the subscription start.
    */
  def aggregateFlow[E: Pretty](
      initialCounterOrPriorEvent: Either[SequencerCounter, PossiblyIgnoredSerializedEvent]
  )(implicit traceContext: TraceContext): Flow[
    OrderedBucketMergeConfig[SequencerId, HasSequencerSubscriptionFactoryAkka[E]],
    Either[SubscriptionControl[E], OrdinarySerializedEvent],
    Future[Done],
  ] = {
    val ops = new SequencerAggregatorMergeOps(initialCounterOrPriorEvent)
    val hub = new OrderedBucketMergeHub[
      SequencerId,
      OrdinarySerializedEvent,
      HasSequencerSubscriptionFactoryAkka[E],
      SequencerCounter,
    ](ops, loggerFactory, enableInvariantCheck)
    Flow.fromGraph(hub).map {
      case OutputElement(elems) => Right(mergeBucket(elems))
      case control: SubscriptionControl[E] =>
        logError(control)
        Left(control)
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
      control: SubscriptionControl[E]
  )(implicit traceContext: TraceContext): Unit =
    control match {
      case ActiveSourceTerminated(sequencerId, cause) =>
        cause.foreach { ex => logger.error(s"Sequencer subscription for $sequencerId failed", ex) }
      case NewConfiguration(_, _) =>
    }

  private class SequencerAggregatorMergeOps[E: Pretty](
      initialCounterOrPriorEvent: Either[SequencerCounter, PossiblyIgnoredSerializedEvent]
  )(implicit val traceContext: TraceContext)
      extends OrderedBucketMergeHubOps[
        SequencerId,
        OrdinarySerializedEvent,
        HasSequencerSubscriptionFactoryAkka[E],
        SequencerCounter,
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
    ): Source[OrdinarySerializedEvent, (KillSwitch, Future[Done])] = {
      val prior = priorElement.collect { case event @ OrdinarySequencedEvent(_, _) => event }
      eventValidator
        .validateAkka(config.subscriptionFactory.create(exclusiveStart), prior, sequencerId)
        .source
        .buffer(bufferSize.value, OverflowStrategy.backpressure)
        .mapConcat(_.unwrap match {
          case Left(err) =>
            // Errors cannot be aggregated because they are specific to a particular sequencer or subscription.
            // So we log them here and do not propagate them.
            // TODO(#13789) Health reporting will pick up the termination of the sequencer connection,
            //  but doesn't need to know the reason for the failure.
            logger.warn(s"Sequencer subscription for $sequencerId failed with $err.")
            // Note that we cannot tunnel the error through the aggregation as a thrown exception
            // because a failure would send a cancellation signal up the stream,
            // which we are trying to avoid for clean shutdown reasons.
            None
          case Right(event) => Some(event)
        })
    }
  }
}

object SequencerAggregatorAkka {
  type SubscriptionControl[E] = ControlOutput[
    SequencerId,
    HasSequencerSubscriptionFactoryAkka[E],
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
}
