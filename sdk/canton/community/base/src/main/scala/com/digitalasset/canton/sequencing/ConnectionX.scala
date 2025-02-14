// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.health.AtomicHealthElement
import com.digitalasset.canton.lifecycle.{FlagCloseable, OnShutdownRunner}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLogging, TracedLogger}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.google.protobuf.ByteString

/** A generic connection. This trait attempts to be independent of the underlying transport.
  *
  * NOTE: We currently make only a minimal effort to keep transport independence, and there are obvious leaks.
  * This will be extended when we need it.
  */
trait ConnectionX extends FlagCloseable with NamedLogging {
  import ConnectionX.*

  def name: String

  def health: ConnectionXHealth

  /** Prepare the underlying transport so that it can be used to make calls. This must be called before sending any
    * command over the connection.
    *
    * The connection to the endpoint is not necessarily established immediately: for example, for
    * gRPC, this call opens a channel, but the connection is only established the first time it is used.
    */
  def start()(implicit traceContext: TraceContext): Unit

  /** Stop the connection by closing the underlying transport's means of communication. Commands cannot
    * be sent after this call until the connection is [[start]]'ed again.
    *
    * For example, for gRPC, this closes the channel.
    */
  def stop()(implicit traceContext: TraceContext): Unit
}

object ConnectionX {

  /** A connection represents just a single endpoint.
    * To provide HA on a logical sequencer, the operator can define multiple connections with the different endpoints.
    * These connections will then be handled in a round-robin load balancing way.
    *
    * @param name an identifier for this connection
    * @param endpoint connection endpoint (host and port)
    * @param transportSecurity whether the connection uses TLS
    * @param customTrustCertificates custom X.509 certificates in PEM format, defined if using TLS
    * @param tracePropagation trace propagation mode used for this connection
    */
  final case class ConnectionXConfig(
      name: String,
      endpoint: Endpoint,
      transportSecurity: Boolean,
      customTrustCertificates: Option[ByteString],
      tracePropagation: TracingConfig.Propagation,
  )

  class ConnectionXHealth(
      override val name: String,
      override val associatedOnShutdownRunner: OnShutdownRunner,
      protected override val logger: TracedLogger,
  ) extends AtomicHealthElement {
    override type State = ConnectionXState

    override protected def prettyState: Pretty[State] = Pretty[State]

    override protected def initialHealthState: State = ConnectionXState.Stopped

    override protected def closingState: State = ConnectionXState.Stopped
  }

  sealed trait ConnectionXError extends Product with Serializable
  object ConnectionXError {

    /** An error happened in the underlying transport.
      */
    final case class TransportError(error: GrpcError) extends ConnectionXError

    /** The connection is in an invalid state.
      */
    final case class InvalidStateError(message: String) extends ConnectionXError
  }

  sealed trait ConnectionXState extends Product with Serializable with PrettyPrinting
  object ConnectionXState {

    /** The connection has started.
      */
    case object Started extends ConnectionXState {
      override protected def pretty: Pretty[Started.type] = prettyOfObject[Started.type]
    }

    /** The connection has stopped.
      */
    case object Stopped extends ConnectionXState {
      override protected def pretty: Pretty[Stopped.type] = prettyOfObject[Stopped.type]
    }
  }
}
