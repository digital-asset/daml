// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service.channel

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.domain.sequencing.service.AuthenticationCheck
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

class GrpcSequencerChannelService(
    authenticationCheck: AuthenticationCheck,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
) extends v30.SequencerChannelServiceGrpc.SequencerChannelService
    with NamedLogging
    with FlagCloseable {
  override def ping(request: v30.PingRequest): Future[v30.PingResponse] =
    Future.successful(v30.PingResponse())

  def disconnectMember(member: Member)(implicit traceContext: TraceContext): Unit = ()
  def disconnectAllMembers()(implicit traceContext: TraceContext): Unit = ()
}
