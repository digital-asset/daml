// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.domain.grpc

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.authentication.grpc.Constant
import com.digitalasset.canton.topology.Member
import io.grpc.ClientCall.Listener
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall
import io.grpc.*

class SequencerConnectClientInterceptor(
    member: Member,
    val loggerFactory: NamedLoggerFactory,
) extends ClientInterceptor
    with NamedLogging {
  override def interceptCall[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
      next: Channel,
  ): ClientCall[ReqT, RespT] =
    new SimpleForwardingClientCall[ReqT, RespT](next.newCall(method, callOptions)) {
      override def start(responseListener: Listener[RespT], headers: Metadata): Unit = {
        headers.put(Constant.MEMBER_ID_METADATA_KEY, member.toProtoPrimitive)

        super.start(responseListener, headers)
      }
    }
}
