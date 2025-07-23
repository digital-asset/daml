// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin

import cats.syntax.bifunctor.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencer.admin.v30.SequencerBftPruningAdministrationServiceGrpc.SequencerBftPruningAdministrationService
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Pruning
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Pruning.{
  KickstartPruning,
  PruningStatusRequest,
}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future, Promise}

class BftOrderingSequencerPruningAdminService(
    pruningModuleRef: ModuleRef[Pruning.Message],
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext,
    metricsContext: MetricsContext,
    traceContext: TraceContext,
) extends SequencerBftPruningAdministrationService
    with NamedLogging {

  override def bftPrune(request: v30.BftPruneRequest): Future[v30.BftPruneResponse] = {
    val promise = Promise[String]()
    for {
      positiveDuration <- convertF(
        PositiveSeconds
          .fromProtoPrimitiveO("retention")(request.retention)
      )
      _ = pruningModuleRef.asyncSend(
        KickstartPruning(positiveDuration.toFiniteDuration, request.minBlocksToKeep, Some(promise))
      )
      msg <- promise.future
    } yield v30.BftPruneResponse(msg)
  }

  def bftPruningStatus(
      request: v30.BftPruningStatusRequest
  ): scala.concurrent.Future[v30.BftPruningStatusResponse] = {
    val promise = Promise[OutputMetadataStore.LowerBound]()
    pruningModuleRef.asyncSend(PruningStatusRequest(promise))
    promise.future.map { lowerBound =>
      v30.BftPruningStatusResponse(
        lowerBoundEpoch = lowerBound.epochNumber,
        lowerBoundBlock = lowerBound.blockNumber,
      )
    }
  }

  private def convertF[T](f: => ProtoConverter.ParsingResult[T])(implicit
      traceContext: TraceContext
  ): Future[T] = f
    .leftMap(err => ProtoDeserializationFailure.Wrap(err).asGrpcError)
    .fold(Future.failed, Future.successful)

}
