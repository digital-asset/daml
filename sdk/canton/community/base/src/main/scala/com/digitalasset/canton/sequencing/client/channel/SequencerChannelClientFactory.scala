// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.grpc.GrpcSequencerChannelBuilder
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth
import com.digitalasset.canton.sequencing.client.{SequencerClient, SequencerClientConfig}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnections,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{DomainId, Member, SequencerId}
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ManagedChannel

import scala.concurrent.ExecutionContextExecutor

/** The SequencerChannelClientFactory creates a SequencerChannelClient and its embedded GRPC channel transports
  */
final class SequencerChannelClientFactory(
    domainId: DomainId,
    crypto: Crypto,
    config: SequencerClientConfig,
    traceContextPropagation: TracingConfig.Propagation,
    domainParameters: StaticDomainParameters,
    processingTimeout: ProcessingTimeout,
    clock: Clock,
    loggerFactory: NamedLoggerFactory,
    supportedProtocolVersions: Seq[ProtocolVersion],
) {
  def create(
      member: Member,
      sequencerConnections: SequencerConnections,
      expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
  )(implicit
      executionContext: ExecutionContextExecutor
  ): Either[String, SequencerChannelClient] =
    makeChannelTransports(
      sequencerConnections,
      member,
      expectedSequencers,
    ).map(transportMap =>
      new SequencerChannelClient(
        member,
        new SequencerChannelClientState(transportMap, processingTimeout, loggerFactory),
        domainParameters,
        processingTimeout,
        loggerFactory,
      )
    )

  private def makeChannelTransports(
      sequencerConnections: SequencerConnections,
      member: Member,
      expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
  )(implicit
      executionContext: ExecutionContextExecutor
  ): Either[String, NonEmpty[Map[SequencerId, SequencerChannelClientTransport]]] = for {
    _ <- {
      val unexpectedSequencers = sequencerConnections.connections.collect {
        case conn if !expectedSequencers.contains(conn.sequencerAlias) =>
          conn.sequencerAlias
      }
      Either.cond(
        unexpectedSequencers.isEmpty,
        (),
        s"Missing sequencer id for alias(es): ${unexpectedSequencers.mkString(", ")}",
      )
    }
    transportsMap = sequencerConnections.connections.map { conn =>
      val sequencerId =
        expectedSequencers.getOrElse(
          conn.sequencerAlias,
          throw new IllegalStateException(
            s"Coding bug: Missing sequencer id for alias ${conn.sequencerAlias} should have been caught above"
          ),
        )
      sequencerId -> makeChannelTransport(conn, sequencerId, member)
    }.toMap
  } yield transportsMap

  private def makeChannelTransport(
      conn: SequencerConnection,
      sequencerId: SequencerId,
      member: Member,
  )(implicit
      executionContext: ExecutionContextExecutor
  ): SequencerChannelClientTransport =
    conn match {
      case connection: GrpcSequencerConnection =>
        val channel = createChannel(connection)
        val auth = grpcSequencerClientAuth(connection, member)
        new SequencerChannelClientTransport(
          channel,
          auth,
          processingTimeout,
          loggerFactory.append("sequencerId", sequencerId.uid.toString),
        )
    }

  private def grpcSequencerClientAuth(
      connection: GrpcSequencerConnection,
      member: Member,
  )(implicit executionContext: ExecutionContextExecutor): GrpcSequencerClientAuth = {
    val channelPerEndpoint = connection.endpoints.map { endpoint =>
      val subConnection = connection.copy(endpoints = NonEmpty.mk(Seq, endpoint))
      endpoint -> createChannel(subConnection)
    }.toMap
    new GrpcSequencerClientAuth(
      domainId,
      member,
      crypto,
      channelPerEndpoint,
      supportedProtocolVersions,
      config.authToken,
      clock,
      processingTimeout,
      SequencerClient.loggerFactoryWithSequencerAlias(
        loggerFactory,
        connection.sequencerAlias,
      ),
    )
  }

  /** Creates a GRPC-level managed channel (not to be confused with a sequencer channel).
    */
  private def createChannel(conn: GrpcSequencerConnection)(implicit
      executionContext: ExecutionContextExecutor
  ): ManagedChannel = {
    val channelBuilder = ClientChannelBuilder(
      SequencerClient.loggerFactoryWithSequencerAlias(loggerFactory, conn.sequencerAlias)
    )
    GrpcSequencerChannelBuilder(
      channelBuilder,
      conn,
      NonNegativeInt.maxValue, // TODO(#21339): Limit and enforce the maximum request/payload size in channels
      traceContextPropagation,
      config.keepAliveClient,
    )
  }
}
