// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import cats.implicits.catsSyntaxEither
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.connection.v30
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc.ApiInfoServiceStub
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencer.api.v30.SequencerConnect.GetSynchronizerParametersResponse.Parameters
import com.digitalasset.canton.sequencer.api.v30.SequencerConnectServiceGrpc.SequencerConnectServiceStub
import com.digitalasset.canton.sequencer.api.v30.{SequencerConnect, SequencerConnectServiceGrpc}
import com.digitalasset.canton.sequencing.SequencerConnectionXClient.SequencerConnectionXClientError
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.topology.{SequencerId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Channel

import scala.concurrent.ExecutionContextExecutor

/** Client to send requests to a sequencer, specialized for gRPC transport.
  */
class GrpcSequencerConnectionXClient(
    connection: GrpcConnectionX,
    apiSvcFactory: Channel => ApiInfoServiceStub,
    sequencerConnectSvcFactory: Channel => SequencerConnectServiceStub,
)(implicit
    ec: ExecutionContextExecutor
) extends SequencerConnectionXClient {
  override def getApiName(retryPolicy: GrpcError => Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXClientError, String] = for {
    apiName <- connection
      .sendRequest(
        requestDescription = "get API info",
        stubFactory = apiSvcFactory,
        retryPolicy = retryPolicy,
      )(_.getApiInfo(v30.GetApiInfoRequest()).map(_.name))
      .leftMap[SequencerConnectionXClientError](
        SequencerConnectionXClientError.ConnectionError.apply
      )
  } yield apiName

  override def performHandshake(
      clientProtocolVersions: NonEmpty[Seq[ProtocolVersion]],
      minimumProtocolVersion: Option[ProtocolVersion],
      retryPolicy: GrpcError => Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXClientError, HandshakeResponse] = {
    val handshakeRequest = HandshakeRequest(clientProtocolVersions, minimumProtocolVersion)
    for {
      handshakeResponseP <- connection
        .sendRequest(
          requestDescription = "perform handshake",
          stubFactory = sequencerConnectSvcFactory,
          retryPolicy = retryPolicy,
        )(_.handshake(handshakeRequest.toProtoV30))
        .leftMap(SequencerConnectionXClientError.ConnectionError.apply)
      handshakeResponse <- EitherT
        .fromEither[FutureUnlessShutdown](HandshakeResponse.fromProtoV30(handshakeResponseP))
        .leftMap[SequencerConnectionXClientError](err =>
          SequencerConnectionXClientError.DeserializationError(err.message)
        )
    } yield handshakeResponse
  }

  override def getSynchronizerAndSequencerIds(retryPolicy: GrpcError => Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXClientError, (SynchronizerId, SequencerId)] =
    for {
      synchronizerIdP <- connection
        .sendRequest(
          requestDescription = "get synchronizer ID",
          stubFactory = sequencerConnectSvcFactory,
          retryPolicy = retryPolicy,
        )(_.getSynchronizerId(SequencerConnect.GetSynchronizerIdRequest()))
        .leftMap(SequencerConnectionXClientError.ConnectionError.apply)

      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        SynchronizerId
          .fromProtoPrimitive(synchronizerIdP.synchronizerId, "synchronizer_id")
          .leftMap(err => SequencerConnectionXClientError.DeserializationError(err.message))
      )

      sequencerId <- EitherT.fromEither[FutureUnlessShutdown](
        UniqueIdentifier
          .fromProtoPrimitive(synchronizerIdP.sequencerUid, "sequencer_uid")
          .map(SequencerId(_))
          .leftMap[SequencerConnectionXClientError](err =>
            SequencerConnectionXClientError.DeserializationError(err.message)
          )
      )
    } yield (synchronizerId, sequencerId)

  override def getStaticSynchronizerParameters(retryPolicy: GrpcError => Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXClientError, StaticSynchronizerParameters] =
    for {
      synchronizerParametersP <- connection
        .sendRequest(
          requestDescription = "get static synchronizer parameters",
          stubFactory = sequencerConnectSvcFactory,
          retryPolicy = retryPolicy,
        )(_.getSynchronizerParameters(SequencerConnect.GetSynchronizerParametersRequest()))
        .leftMap(SequencerConnectionXClientError.ConnectionError.apply)

      synchronizerParametersE = synchronizerParametersP.parameters match {
        case Parameters.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("GetSynchronizerParameters.parameters"))
        case Parameters.ParametersV1(parametersV1) =>
          StaticSynchronizerParameters.fromProtoV30(parametersV1)
      }
      synchronizerParameters <- EitherT.fromEither[FutureUnlessShutdown](
        synchronizerParametersE
          .leftMap[SequencerConnectionXClientError](err =>
            SequencerConnectionXClientError.DeserializationError(err.message)
          )
      )
    } yield synchronizerParameters
}

object SequencerConnectionXClientFactoryImpl extends SequencerConnectionXClientFactory {
  override def create(connection: ConnectionX)(implicit
      ec: ExecutionContextExecutor
  ): SequencerConnectionXClient = connection match {
    case grpcConnection: GrpcConnectionX =>
      new GrpcSequencerConnectionXClient(
        grpcConnection,
        ApiInfoServiceGrpc.stub,
        SequencerConnectServiceGrpc.stub,
      )

    case _ => throw new IllegalStateException(s"Connection type not supported: $connection")
  }
}
