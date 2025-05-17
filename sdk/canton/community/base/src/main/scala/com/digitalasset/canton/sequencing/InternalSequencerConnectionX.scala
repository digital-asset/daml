// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.health.HealthElement
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasRunOnClosing}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLogging, TracedLogger}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.ConnectionX.ConnectionXConfig
import com.digitalasset.canton.sequencing.SequencerConnectionXStub.SequencerConnectionXStubError
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContextExecutor

/** A generic connection to a sequencer. This trait attempts to be independent of the underlying
  * transport.
  *
  * This is an internal class used by the connection pool. Other components are expected to interact
  * with the sequencer using the higher-level [[SequencerConnectionX]].
  *
  * NOTE: We currently make only a minimal effort to keep transport independence, and there are
  * obvious leaks. This will be extended when we need it.
  */
trait InternalSequencerConnectionX extends FlagCloseable with NamedLogging {
  import InternalSequencerConnectionX.*

  def name: String

  def config: ConnectionXConfig

  def health: SequencerConnectionXHealth

  /** Return the attributes of this sequencer connection.
    * @return
    *   `None` if the sequencer connection has not yet been validated.
    */
  def attributes: Option[ConnectionAttributes]

  def tryAttributes: ConnectionAttributes =
    attributes.getOrElse(throw new IllegalStateException(s"Connection $name has no attributes"))

  /** Start the connection
    */
  def start()(implicit traceContext: TraceContext): Either[SequencerConnectionXError, Unit]

  /** Stop the connection for a non-fatal reason
    */
  def fail(reason: String)(implicit traceContext: TraceContext): Unit

  /** Stop the connection for a fatal reason. The connection should not be restarted.
    */
  def fatal(reason: String)(implicit traceContext: TraceContext): Unit

  /** Build a user connection from this internal connection.
    * @param authConfig
    *   configuration for the authentication manager
    * @param member
    *   member for which to authenticate
    * @param crypto
    *   crypto to use for authentication
    */
  def buildUserConnection(
      authConfig: AuthenticationTokenManagerConfig,
      member: Member,
      crypto: SynchronizerCrypto,
      clock: Clock,
  ): Either[SequencerConnectionXError, SequencerConnectionX]
}

object InternalSequencerConnectionX {
  abstract class SequencerConnectionXHealth(
      override val name: String,
      override val associatedHasRunOnClosing: HasRunOnClosing,
      protected override val logger: TracedLogger,
  ) extends HealthElement {
    override type State = SequencerConnectionXState

    override protected def prettyState: Pretty[State] = Pretty[State]

    override protected def initialHealthState: State = SequencerConnectionXState.Initial

    override protected def closingState: State = SequencerConnectionXState.Fatal
  }

  sealed trait SequencerConnectionXInternalError extends Product with Serializable
  object SequencerConnectionXInternalError {

    /** An error happened when sending a request to the sequencer.
      */
    final case class StubError(error: SequencerConnectionXStubError)
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
    *    ┌─────────┐       ┌─────────┐        ┌─────────┐         ┌─────────┐
    *    │ INITIAL │       │STARTING │        │ STARTED │         │VALIDATED│
    *    ├─────────┤       ├─────────┤        ├─────────┤         ├─────────┤
    *    │         ├───────►         ├────────►         ├─────────►         │
    *    │         │       │         │        │         │         │         │      from
    *    │         │       │         ◄─────┐  │         │         │         │     anywhere
    *    │         │       │         ├──┐  │  │         │         │         │        │
    *    └─────────┘       └────▲────┘  │  │  └────┬────┘         └────┬────┘        │
    *                           │       │  │       │                   │             │
    *                           │       │  │       │                   │        ┌────▼────┐
    *                           │       │  │       │                   │        │  FATAL  │
    *                           │       │  │       │                   │        ├─────────┤
    *                      ┌────┴────┐  │  │  ┌────▼────┐              │        │         │
    *                      │ STOPPED │  │  │  │STOPPING │              │        │         │
    *                      ├─────────┤  │  │  ├─────────┤              │        │         │
    *                      │         │  │  └──┤         │              │        │         │
    *                      │         │  └─────►         ◄──────────────┘        └─────────┘
    *                      │         ◄────────┤         │
    *                      │         │        │         │
    *                      └─────────┘        └─────────┘
    * }}}
    */
  sealed trait SequencerConnectionXState extends Product with Serializable with PrettyPrinting
  object SequencerConnectionXState {

    /** Initial state of the sequencer connection after creation. */
    case object Initial extends SequencerConnectionXState {
      override protected def pretty: Pretty[Initial.type] = prettyOfObject[Initial.type]
    }

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
      synchronizerId: PhysicalSynchronizerId,
      sequencerId: SequencerId,
      staticParameters: StaticSynchronizerParameters,
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[ConnectionAttributes] = prettyOfClass(
      param("physical synchronizer id", _.synchronizerId),
      param("sequencer", _.sequencerId),
      param("static parameters", _.staticParameters),
    )
  }
}

trait InternalSequencerConnectionXFactory {
  def create(connectionXConfig: ConnectionXConfig)(implicit
      ec: ExecutionContextExecutor
  ): InternalSequencerConnectionX
}
