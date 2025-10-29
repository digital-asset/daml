// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.platform.store.PruningOffsetService
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

final case class PruningOffsetServiceImpl(
    participantPruningStore: ParticipantPruningStore,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends PruningOffsetService
    with NamedLogging {

  override def pruningOffset(implicit
      traceContext: TraceContext
  ): Future[Option[Offset]] =
    participantPruningStore
      .pruningStatus()
      .map(_.startedO)
      .failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)

}
