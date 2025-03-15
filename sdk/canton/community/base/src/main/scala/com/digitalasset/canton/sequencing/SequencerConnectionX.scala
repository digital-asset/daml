// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.health.HealthElement
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasRunOnClosing}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLogging, TracedLogger}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.SequencerConnectionXClient.SequencerConnectionXClientError
import com.digitalasset.canton.topology.{SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext

/** A generic connection to a sequencer. This trait attempts to be independent of the underlying
  * transport.
  *
  * NOTE: We currently make only a minimal effort to keep transport independence, and there are
  * obvious leaks. This will be extended when we need it.
  */
trait SequencerConnectionX extends FlagCloseable with NamedLogging {
  import SequencerConnectionX.*

  def name: String

  def health: SequencerConnectionXHealth

  /** Return the attributes of this sequencer connection.
    * @return
    *   `None` if the sequencer connection has not yet been validated.
    */
  def attributes: Option[ConnectionAttributes]

  /** Start the connection
    */
  def start()(implicit traceContext: TraceContext): Either[SequencerConnectionXError, Unit]

  /** Stop the connection for a non-fatal reason
    */
  def fail(reason: String)(implicit traceContext: TraceContext): Unit

  /** Stop the connection for a fatal reason. The connection should not be restarted.
    */
  def fatal(reason: String)(implicit traceContext: TraceContext): Unit
}

object SequencerConnectionX {
  abstract class SequencerConnectionXHealth(
      override val name: String,
      override val associatedHasRunOnClosing: HasRunOnClosing,
      protected override val logger: TracedLogger,
  ) extends HealthElement {
    override type State = SequencerConnectionXState

    override protected def prettyState: Pretty[State] = Pretty[State]

    override protected def initialHealthState: State = SequencerConnectionXState.Stopped

    override protected def closingState: State = SequencerConnectionXState.Fatal
  }

  sealed trait SequencerConnectionXInternalError extends Product with Serializable
  object SequencerConnectionXInternalError {

    /** An error happened when sending a request to the sequencer.
      */
    final case class ClientError(error: SequencerConnectionXClientError)
        extends SequencerConnectionXInternalError

    /** An error happened while validating the sequencer connection.
      */
    final case class ValidationError(message: String) extends SequencerConnectionXInternalError
  }

  sealed trait SequencerConnectionXError extends Product with Serializable
  object SequencerConnectionXError {

    /** The connection is in an invalid state.
      */
    final case class InvalidStateError(message: String) extends SequencerConnectionXError
  }

  /** {{{
    *     ┌─────────┐        ┌─────────┐         ┌─────────┐
    *     │STARTING │        │ STARTED │         │VALIDATED│
    *     ├─────────┤        ├─────────┤         ├─────────┤
    *     │         ├────────►         ├─────────►         │
    *     │         │        │         │         │         │      from
    *     │         ├─────┐  │         │         │         │     anywhere
    *     │         ◄──┐  │  │         │         │         │        │
    *     └────▲────┘  │  │  └────┬────┘         └────┬────┘        │
    *          │       │  │       │                   │             │
    *          │       │  │       │                   │        ┌────▼────┐
    *          │       │  │       │                   │        │  FATAL  │
    *          │       │  │       │                   │        ├─────────┤
    *     ┌────┴────┐  │  │  ┌────▼────┐              │        │         │
    *     │ STOPPED │  │  │  │STOPPING │              │        │         │
    *     ├─────────┤  │  │  ├─────────┤              │        │         │
    *     │         │  │  └──┤         │              │        │         │
    *     │         │  └─────►         ◄──────────────┘        └─────────┘
    *     │         ◄────────┤         │
    *     │         │        │         │
    *     └─────────┘        └─────────┘
    * }}}
    */
  sealed trait SequencerConnectionXState extends Product with Serializable with PrettyPrinting
  object SequencerConnectionXState {

    /** The sequencer connection is starting. */
    case object Starting extends SequencerConnectionXState {
      override protected def pretty: Pretty[Starting.type] = prettyOfObject[Starting.type]
    }

    /** The sequencer connection has started, but has not yet been validated. */
    case object Started extends SequencerConnectionXState {
      override protected def pretty: Pretty[Started.type] = prettyOfObject[Started.type]
    }

    /** The sequencer connection has been validated. */
    case object Validated extends SequencerConnectionXState {
      override protected def pretty: Pretty[Validated.type] = prettyOfObject[Validated.type]
    }

    /** The sequencer connection is stopping. */
    case object Stopping extends SequencerConnectionXState {
      override protected def pretty: Pretty[Stopping.type] = prettyOfObject[Stopping.type]
    }

    /** The sequencer connection has either not been started yet, or has stopped for a non-fatal
      * reason. (Re)starting it will possibly bring it back up.
      */
    case object Stopped extends SequencerConnectionXState {
      override protected def pretty: Pretty[Stopped.type] = prettyOfObject[Stopped.type]
    }

    /** The sequencer connection has stopped for a fatal reason. It should not be restarted. */
    case object Fatal extends SequencerConnectionXState {
      override protected def pretty: Pretty[Fatal.type] = prettyOfObject[Fatal.type]
    }
  }

  /** Attributes of this sequencer connection.
    */
  final case class ConnectionAttributes(
      synchronizerId: SynchronizerId,
      sequencerId: SequencerId,
      staticParameters: StaticSynchronizerParameters,
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[ConnectionAttributes] = prettyOfClass(
      param("synchronizer", _.synchronizerId),
      param("sequencer", _.sequencerId),
      param("static parameters", _.staticParameters),
    )
  }
}
