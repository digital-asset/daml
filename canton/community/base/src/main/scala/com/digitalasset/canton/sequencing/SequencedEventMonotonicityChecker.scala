// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import akka.NotUsed
import akka.stream.scaladsl.Flow
import cats.syntax.functorFilter.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.AkkaUtil.WithKillSwitch
import com.digitalasset.canton.util.ErrorUtil
import com.google.common.annotations.VisibleForTesting

/** Checks that the sequenced events' sequencer counters are a gap-free increasing sequencing starting at `firstSequencerCounter`
  * and their timestamps increase strictly monotonically. When a violation is detected, an error is logged and
  * the processing is aborted.
  *
  * This is normally ensured by the [[com.digitalasset.canton.sequencing.client.SequencedEventValidator]] for individual sequencer subscriptions.
  * However, due to aggregating multiple subscriptions from several sequencers up to a threshold,
  * the stream of events emitted by the aggregation may violate monotonicity. This additional monotonicity check
  * ensures that we catch such violations before we pass the events downstream.
  */
class SequencedEventMonotonicityChecker(
    firstSequencerCounter: SequencerCounter,
    firstTimestampLowerBoundInclusive: CantonTimestamp,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  import SequencedEventMonotonicityChecker.*

  /** Akka version of the check. Pulls the kill switch and drains the source when a violation is detected. */
  def flow: Flow[
    WithKillSwitch[OrdinarySerializedEvent],
    WithKillSwitch[OrdinarySerializedEvent],
    NotUsed,
  ] = {
    Flow[WithKillSwitch[OrdinarySerializedEvent]]
      .statefulMap(() => initialState)(
        (state, eventAndKillSwitch) => eventAndKillSwitch.traverse(onNext(state, _)),
        _ => None,
      )
      .mapConcat { actionAndKillSwitch =>
        actionAndKillSwitch.traverse {
          case Emit(event) => Some(event)
          case failure: MonotonicityFailure =>
            implicit val traceContext: TraceContext = failure.event.traceContext
            logger.error(failure.message)
            actionAndKillSwitch.killSwitch.shutdown()
            None
          case Drop => None
        }
      }
  }

  /** [[com.digitalasset.canton.sequencing.ApplicationHandler]] version.
    * @throws com.digitalasset.canton.sequencing.SequencedEventMonotonicityChecker.MonotonicityFailureException
    *   when a monotonicity violation is detected
    */
  def handler(
      handler: OrdinaryApplicationHandler[ClosedEnvelope]
  ): OrdinaryApplicationHandler[ClosedEnvelope] = {
    // Application handlers must be called sequentially, so a plain var is good enough here
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var state: State = initialState
    handler.replace { tracedEvents =>
      val filtered = tracedEvents.map(_.mapFilter { event =>
        val (nextState, action) = onNext(state, event)
        state = nextState
        action match {
          case Emit(_) => Some(event)
          case failure: MonotonicityFailure =>
            implicit val traceContext: TraceContext = event.traceContext
            ErrorUtil.internalError(failure.asException)
          case Drop => None
        }
      })
      handler.apply(filtered)
    }
  }

  private def initialState: State =
    GoodState(firstSequencerCounter, firstTimestampLowerBoundInclusive)

  private def onNext(state: State, event: OrdinarySerializedEvent): (State, Action) = state match {
    case Failed => (state, Drop)
    case GoodState(nextSequencerCounter, lowerBoundTimestamp) =>
      val monotonic =
        event.counter == nextSequencerCounter && event.timestamp >= lowerBoundTimestamp
      if (monotonic) {
        val nextState = GoodState(event.counter + 1, event.timestamp.immediateSuccessor)
        nextState -> Emit(event)
      } else {
        val error = MonotonicityFailure(nextSequencerCounter, lowerBoundTimestamp, event)
        Failed -> error
      }
  }
}

object SequencedEventMonotonicityChecker {

  private sealed trait Action extends Product with Serializable
  private final case class Emit(event: OrdinarySerializedEvent) extends Action
  private case object Drop extends Action
  private final case class MonotonicityFailure(
      expectedSequencerCounter: SequencerCounter,
      timestampLowerBound: CantonTimestamp,
      event: OrdinarySerializedEvent,
  ) extends Action {
    def message: String =
      s"Sequencer counters and timestamps do not increase monotonically. Expected next counter=$expectedSequencerCounter with timestamp lower bound $timestampLowerBound, but received ${event.signedEvent.content}"

    def asException: Exception = new MonotonicityFailureException(message)
  }
  @VisibleForTesting
  class MonotonicityFailureException(message: String) extends Exception(message)

  private sealed trait State extends Product with Serializable
  private case object Failed extends State
  private final case class GoodState(
      nextSequencerCounter: SequencerCounter,
      lowerBoundTimestamp: CantonTimestamp,
  ) extends State
}
