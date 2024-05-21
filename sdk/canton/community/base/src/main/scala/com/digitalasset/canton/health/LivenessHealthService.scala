// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.pretty.Pretty.*
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.health.v1.HealthCheckResponse.ServingStatus

/** A [[LivenessHealthService]] aggregates [[CloseableHealthComponent]]s under dependencies.
  * Once switching to [[io.grpc.health.v1.HealthCheckResponse.ServingStatus.NOT_SERVING]] it will not switch back.
  * Services are queryable through their name in the gRPC Health Check service.
  * Dependencies are reported under their names too.
  *
  * The state of the [[LivenessHealthService]] is [[io.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING]]
  * if and only if none of the dependencies have fatally failed (have their state [[ComponentHealthState.Fatal]].
  */
final class LivenessHealthService(
    override protected val logger: TracedLogger,
    override protected val timeouts: ProcessingTimeout,
    private val fatalDependencies: Seq[HealthQuasiComponent],
) extends HealthService {

  override val name: String = LivenessHealthService.Name

  alterDependencies(
    remove = Set.empty,
    add = fatalDependencies.map(dep => dep.name -> dep).toMap,
  )

  override protected def closingState: ServingStatus = getState
  override protected def prettyState: Pretty[ServingStatus] = Pretty[ServingStatus]

  override protected def refreshFromDependencies()(implicit traceContext: TraceContext): Unit = {
    if (getState == ServingStatus.SERVING) {
      super.refreshFromDependencies()
    }
  }

  override protected def combineDependentStates: ServingStatus = {
    if (fatalDependencies.forall(!_.isFatal)) ServingStatus.SERVING
    else ServingStatus.NOT_SERVING
  }

  override protected def initialHealthState: ServingStatus = ServingStatus.SERVING

  def dependencies: Seq[HealthQuasiComponent] = fatalDependencies
}

object LivenessHealthService {

  val Name: String = "liveness"
  def apply(
      logger: TracedLogger,
      timeouts: ProcessingTimeout,
      fatalDependencies: Seq[HealthQuasiComponent],
  ): LivenessHealthService =
    new LivenessHealthService(logger, timeouts, fatalDependencies)

  def alwaysAlive(
      logger: TracedLogger,
      timeouts: ProcessingTimeout,
  ): LivenessHealthService =
    new LivenessHealthService(logger, timeouts, fatalDependencies = Seq.empty)

  implicit val prettyServiceHealth: Pretty[LivenessHealthService] = prettyOfClass(
    param("name", _.name.unquoted),
    param("state", _.getState),
  )
}
