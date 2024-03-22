// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication.grpc

import cats.implicits.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.authentication.grpc.Constant
import com.digitalasset.canton.topology.Member
import io.grpc.*

class SequencerConnectServerInterceptor(val loggerFactory: NamedLoggerFactory)
    extends ServerInterceptor
    with NamedLogging {

  override def interceptCall[ReqT, RespT](
      serverCall: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*

    Option(headers.get(Constant.MEMBER_ID_METADATA_KEY)) match {
      case None => next.startCall(serverCall, headers)

      case Some(memberId) =>
        new AsyncForwardingListener[ReqT] {
          // taking the context before an async callback happening (likely on another thread) is critical for maintaining
          // any original context values
          val originalContext = Context.current()

          Member
            .fromProtoPrimitive(memberId, "memberId")
            .leftMap(err => s"Failed to deserialize member id: $err") match {
            case Left(error) =>
              logger.warn(error)
              setNextListener(failVerification(error, serverCall, headers))

            case Right(member) =>
              val contextWithAuthorizedMember = originalContext
                .withValue(IdentityContextHelper.storedMemberContextKey, Some(member))

              setNextListener(
                Contexts.interceptCall(contextWithAuthorizedMember, serverCall, headers, next)
              )
          }

        }
    }
  }

  private def failVerification[ReqT, RespT](
      msg: String,
      serverCall: ServerCall[ReqT, RespT],
      headers: Metadata,
  ): ServerCall.Listener[ReqT] = {
    serverCall.close(Status.INVALID_ARGUMENT.withDescription(msg), headers)
    new ServerCall.Listener[ReqT]() {}
  }
}
