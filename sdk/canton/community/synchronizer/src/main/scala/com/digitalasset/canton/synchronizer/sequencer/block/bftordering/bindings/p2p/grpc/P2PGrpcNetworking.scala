// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.TlsClientConfig
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.synchronizer.sequencer.AuthenticationServices
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver

import scala.util.Try
import scala.util.control.NonFatal

object P2PGrpcNetworking {

  /** The BFT orderer's internal representation of a P2P network endpoint.
    *
    * Non-networked tests (unit and simulation tests) use the [[PlainTextP2PEndpoint]] concrete
    * subclass and fill in fake ports.
    *
    * It is possible to generalize further and introduce a symbolic endpoint type for non-networked
    * tests, so that they don't have to deal with networking-only information, but it's not worth
    * the added complexity and required time/maintenance investment, because:
    *
    *   - The endpoint type should become part of the environment definition, and endpoint admin
    *     messages would thus become parametric in the environment.
    *   - Furthermore, endpoint administration is only supported for networked endpoints, so the
    *     network output module would have to be split into a networked and a non-networked part and
    *     networked-only functionality should be tested separately.
    */
  sealed trait P2PEndpoint extends Product {

    def address: String
    def port: Port
    def transportSecurity: Boolean
    def endpointConfig: BftBlockOrdererConfig.P2PEndpointConfig

    final lazy val id: P2PEndpoint.Id = P2PEndpoint.Id(address, port, transportSecurity)
  }

  object P2PEndpoint {

    final case class Id(
        address: String,
        port: Port,
        transportSecurity: Boolean,
    ) extends Ordered[Id]
        with Product
        with PrettyPrinting {

      lazy val url = s"${if (transportSecurity) "https" else "http"}://$address:$port"

      override def compare(that: Id): Int =
        Id.unapply(this).compare(Id.unapply(that))

      override protected def pretty: Pretty[Id] =
        prettyOfClass(param("url", _.url.doubleQuoted), param("tls", _.transportSecurity))
    }

    def fromEndpointConfig(
        config: BftBlockOrdererConfig.P2PEndpointConfig
    ): P2PEndpoint =
      config.tlsConfig match {
        case Some(TlsClientConfig(_, _, enabled)) =>
          if (!enabled)
            PlainTextP2PEndpoint.fromEndpointConfig(config)
          else
            TlsP2PEndpoint.fromEndpointConfig(config)
        case _ =>
          PlainTextP2PEndpoint.fromEndpointConfig(config)
      }
  }

  final case class PlainTextP2PEndpoint(
      override val address: String,
      override val port: Port,
  ) extends P2PEndpoint {

    override val transportSecurity: Boolean = false

    override lazy val endpointConfig: BftBlockOrdererConfig.P2PEndpointConfig =
      BftBlockOrdererConfig.P2PEndpointConfig(
        address,
        port,
        Some(TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = false)),
      )
  }

  object PlainTextP2PEndpoint {

    private[p2p] def fromEndpointConfig(
        config: BftBlockOrdererConfig.P2PEndpointConfig
    ): PlainTextP2PEndpoint =
      PlainTextP2PEndpoint(config.address, config.port)
  }

  final case class TlsP2PEndpoint(
      override val endpointConfig: BftBlockOrdererConfig.P2PEndpointConfig
  ) extends P2PEndpoint {

    override val transportSecurity: Boolean = true

    override val address: String = endpointConfig.address

    override def port: Port = endpointConfig.port
  }

  object TlsP2PEndpoint {

    private[p2p] def fromEndpointConfig(
        endpointConfig: BftBlockOrdererConfig.P2PEndpointConfig
    ): TlsP2PEndpoint =
      TlsP2PEndpoint(endpointConfig)
  }

  private[bftordering] final case class AuthenticationInitialState(
      psId: PhysicalSynchronizerId,
      sequencerId: SequencerId,
      authenticationServices: AuthenticationServices,
      authTokenConfig: AuthenticationTokenManagerConfig,
      serverToClientAuthenticationEndpoint: Option[P2PEndpoint],
      clock: Clock,
  )

  private[grpc] def completeGrpcStreamObserver(
      streamObserver: StreamObserver[?],
      logger: TracedLogger,
  )(implicit traceContext: TraceContext): Unit =
    completeWith(streamObserver.toString, logger)(() => streamObserver.onCompleted())

  private[grpc] def failGrpcStreamObserver(
      streamObserver: StreamObserver[?],
      throwable: Throwable,
      logger: TracedLogger,
  )(implicit traceContext: TraceContext): Unit =
    completeWith(streamObserver.toString, logger)(() => streamObserver.onError(throwable))

  private def completeWith(description: String, logger: TracedLogger)(
      complete: () => Unit
  )(implicit traceContext: TraceContext): Unit =
    Try(complete()).recover {
      case NonFatal(t) =>
        logger.debug(
          s"Could not complete '$description' due to a regular exception, likely already completed",
          t,
        )
      case t: Throwable =>
        logger.error(s"Could not complete '$description' due to a fatal exception", t)
        throw t
    }.discard
}
