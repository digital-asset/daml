// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.PekkoUtil.WithKillSwitch
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

/** Checks that the sequenced events' stream is gap-free:
  *   - We expect event timestamps to be monotonically increasing
  *   - The sequence should start with `previousTimestamp = None`.
  *   - For each subsequent event, we expect `previousTimestamp` to be set to the timestamp of the
  *     previous event.
  *   - When a violation is detected, an error is logged and the processing is aborted.
  *
  * This is normally ensured by the
  * [[com.digitalasset.canton.sequencing.client.SequencedEventValidator]] for individual sequencer
  * subscriptions. However, due to aggregating multiple subscriptions from several sequencers up to
  * a threshold, the stream of events emitted by the aggregation may violate monotonicity. This
  * additional monotonicity check ensures that we catch such violations before we pass the events
  * downstream.
  */
class SequencedEventMonotonicityChecker(
    previousEventTimestamp: Option[CantonTimestamp],
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  import SequencedEventMonotonicityChecker.*

  /** Pekko version of the check. Pulls the kill switch and drains the source when a violation is
    * detected.
    */
  def flow[E]: Flow[
    WithKillSwitch[Either[E, SequencedSerializedEvent]],
    WithKillSwitch[Either[E, SequencedSerializedEvent]],
    NotUsed,
  ] =
    Flow[WithKillSwitch[Either[E, SequencedSerializedEvent]]]
      .statefulMap(() => initialState)(
        (state, eventAndKillSwitch) =>
          eventAndKillSwitch.traverse {
            case left @ Left(_) => state -> Emit(left)
            case Right(event) => onNext(state, event).map(_.map(_ => Right(event)))
          },
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

  /** [[com.digitalasset.canton.sequencing.ApplicationHandler]] version.
    * @throws com.digitalasset.canton.sequencing.SequencedEventMonotonicityChecker.MonotonicityFailureException
    *   when a monotonicity violation is detected
    */
  def handler(
      handler: SequencedApplicationHandler[ClosedEnvelope]
  ): SequencedApplicationHandler[ClosedEnvelope] = {
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

  private def initialState: State = GoodState(previousEventTimestamp)

  private def onNext(
      state: State,
      event: SequencedSerializedEvent,
  ): (State, Action[SequencedSerializedEvent]) = state match {
    case Failed => (state, Drop)
    case GoodState(previousEventTimestamp) =>
      // Note that here we only check the monotonicity of the event timestamps,
      // not the presence of gaps in the event stream by checking the previousTimestamp.
      // That is done by the SequencedEventValidator, which checks for the fork
      val monotonic = previousEventTimestamp.forall { previous =>
        event.timestamp > previous
      }
      if (monotonic) {
        val nextState = GoodState(Some(event.timestamp))
        nextState -> Emit(event)
      } else {
        val error = MonotonicityFailure(previousEventTimestamp, event)
        Failed -> error
      }
  }
}

object SequencedEventMonotonicityChecker {

  private sealed trait Action[+A] extends Product with Serializable {
    def map[B](f: A => B): Action[B]
  }
  private final case class Emit[+A](event: A) extends Action[A] {
    override def map[B](f: A => B): Emit[B] = Emit(f(event))
  }
  private case object Drop extends Action[Nothing] {
    override def map[B](f: Nothing => B): this.type = this
  }
  private final case class MonotonicityFailure(
      previousEventTimestamp: Option[CantonTimestamp],
      event: SequencedSerializedEvent,
  ) extends Action[Nothing] {
    def message: String =
      s"Timestamps do not increase monotonically or previous event timestamp does not match. Expected previousTimestamp=$previousEventTimestamp, but received ${event.signedEvent.content}"

    def asException: Exception = new MonotonicityFailureException(message)

    override def map[B](f: Nothing => B): this.type = this
  }
  @VisibleForTesting
  class MonotonicityFailureException(message: String) extends Exception(message)

  private sealed trait State extends Product with Serializable
  private case object Failed extends State
  private final case class GoodState(
      previousEventTimestamp: Option[CantonTimestamp]
  ) extends State
}
