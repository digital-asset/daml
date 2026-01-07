// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.admin.grpc.{GrpcPruningScheduler, HasPruningScheduler}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencer.admin.v30.SequencerBftPruningAdministrationServiceGrpc.SequencerBftPruningAdministrationService
import com.digitalasset.canton.sequencer.admin.v30.{
  GetBftScheduleRequest,
  GetBftScheduleResponse,
  SetBftScheduleRequest,
  SetBftScheduleResponse,
  SetMinBlocksToKeepRequest,
  SetMinBlocksToKeepResponse,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.{
  BftOrdererPruningSchedule,
  BftOrdererPruningScheduler,
  BftPruningStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Pruning
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Pruning.{
  KickstartPruning,
  PruningStatusRequest,
}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.{ExecutionContext, Future, Promise}

class GrpcBftOrderingSequencerPruningAdminService(
    pruningModuleRef: ModuleRef[Pruning.Message],
    pruningScheduler: BftOrdererPruningScheduler,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext,
    metricsContext: MetricsContext,
    traceContext: TraceContext,
) extends SequencerBftPruningAdministrationService
    with HasPruningScheduler
    with GrpcPruningScheduler
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
    val promise = Promise[BftPruningStatus]()
    pruningModuleRef.asyncSend(PruningStatusRequest(promise))
    promise.future.map(_.toProto)
  }

  override def setBftSchedule(request: SetBftScheduleRequest): Future[SetBftScheduleResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      bftOrdererSchedule <- convertRequiredF(
        "bft_orderer_schedule",
        request.schedule,
        BftOrdererPruningSchedule.fromProtoV30,
      )
      _ <- handlePassiveHAStorageError(
        pruningScheduler.setBftOrdererSchedule(bftOrdererSchedule),
        "set_bft_orderer_schedule",
      )
    } yield v30.SetBftScheduleResponse()
  }

  override def getBftSchedule(request: GetBftScheduleRequest): Future[GetBftScheduleResponse] =
    for {
      schedule <- pruningScheduler.getBftOrdererSchedule()
    } yield GetBftScheduleResponse(schedule.map(_.toProtoV30))

  override def setMinBlocksToKeep(
      request: SetMinBlocksToKeepRequest
  ): Future[SetMinBlocksToKeepResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      _ <- handleUserError(pruningScheduler.updateMinBlocksToKeep(request.minBlocksToKeep))
    } yield v30.SetMinBlocksToKeepResponse()
  }

  override protected def ensureScheduler(implicit
      traceContext: TraceContext
  ): Future[PruningScheduler] = Future.successful(pruningScheduler)

}
