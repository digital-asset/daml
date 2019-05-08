// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.grpcHeaders

import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ClientInterceptors
import io.grpc.Metadata
import io.grpc.MethodDescriptor

class GrpcHeadersClientInterceptor(grpcHeaderDataInjector: GrpcHeadersDataOrigin)
    extends ClientInterceptor {

  private[grpcHeaders] def interceptCall[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
      next: Channel): ClientCall[ReqT, RespT] =
    new HeaderAttachingClientCall[ReqT, RespT](next.newCall(method, callOptions))

  final private class HeaderAttachingClientCall[ReqT, RespT](val call: ClientCall[ReqT, RespT])
      extends ClientInterceptors.CheckedForwardingClientCall[ReqT, RespT](call) {
    override protected def checkedStart(
        responseListener: ClientCall.Listener[RespT],
        headers: Metadata): Unit = {
      headers.merge(grpcHeaderDataInjector.getAdditionalHeaderData)
      delegate.start(responseListener, headers)
    }
  }
}
