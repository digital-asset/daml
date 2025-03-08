// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.authentication

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencer.admin.v30.PeerEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.{
  endpointFromProto,
  endpointToProto,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import io.grpc.Metadata.BinaryMarshaller
import io.grpc.{
  CallOptions,
  Channel,
  ClientCall,
  ClientInterceptor,
  ForwardingClientCall,
  Metadata,
  MethodDescriptor,
}

private[networking] class AddEndpointHeaderClientInterceptor(
    serverEndpoint: P2PEndpoint,
    override val loggerFactory: NamedLoggerFactory,
) extends ClientInterceptor
    with NamedLogging {

  import AddEndpointHeaderClientInterceptor.ENDPOINT_METADATA_KEY

  override def interceptCall[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
      next: Channel,
  ): ClientCall[ReqT, RespT] =
    new ForwardingClientCall.SimpleForwardingClientCall[ReqT, RespT](
      next.newCall(method, callOptions)
    ) {
      override def start(responseListener: ClientCall.Listener[RespT], headers: Metadata): Unit = {
        implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
        logger.debug(s"Adding server endpoint header to outgoing call: $serverEndpoint")
        headers.put(ENDPOINT_METADATA_KEY, serverEndpoint)
        super.start(responseListener, headers)
      }
    }
}

private[bftordering] object AddEndpointHeaderClientInterceptor {

  private val ENDPOINT_METADATA_MARSHALLER =
    new BinaryMarshaller[P2PEndpoint] {
      override def toBytes(value: P2PEndpoint): Array[Byte] =
        endpointToProto(value).toByteArray
      override def parseBytes(serialized: Array[Byte]): P2PEndpoint =
        endpointFromProto(PeerEndpoint.parseFrom(serialized))
          .fold(parseError => throw new IllegalArgumentException(parseError), identity)
    }

  val ENDPOINT_METADATA_KEY: Metadata.Key[P2PEndpoint] =
    Metadata.Key.of(
      s"${classOf[P2PEndpoint].getName.replace("$", "_")}-${Metadata.BINARY_HEADER_SUFFIX}",
      ENDPOINT_METADATA_MARSHALLER,
    )
}
