// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.pretty.Pretty.*
import io.grpc.health.v1.HealthCheckResponse.ServingStatus

/** A [[HealthService]] aggregates [[CloseableHealthComponent]]s under critical and soft dependencies.
  * Services are queryable through their name in the gRPC Health Check service.
  * Both critical and soft dependencies are reported under their names too.
  *
  * The state of the [[HealthService]] is [[io.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING]]
  * if and only if none of the critical dependencies have failed. Soft dependencies are merely reported
  * as dependencies, but do not influence the status of the [[HealthService]] itself.
  */
final class HealthService(
    override val name: String,
    override protected val logger: TracedLogger,
    override protected val timeouts: ProcessingTimeout,
    private val criticalDependencies: Seq[HealthQuasiComponent],
    private val softDependencies: Seq[HealthQuasiComponent],
) extends CloseableHealthElement
    with CompositeHealthElement[String, HealthQuasiComponent] {

  alterDependencies(
    remove = Set.empty,
    add = criticalDependencies.map(dep => dep.name -> dep).toMap,
  )

  override protected def closingState: ServingStatus = ServingStatus.NOT_SERVING

  override type State = ServingStatus
  override protected def prettyState: Pretty[ServingStatus] = Pretty[ServingStatus]

  override protected def combineDependentStates: ServingStatus = {
    if (criticalDependencies.forall(!_.isFailed)) ServingStatus.SERVING
    else ServingStatus.NOT_SERVING
  }

  override protected def initialHealthState: ServingStatus =
    if (criticalDependencies.isEmpty) ServingStatus.SERVING else ServingStatus.NOT_SERVING

  def dependencies: Seq[HealthQuasiComponent] = criticalDependencies ++ softDependencies
}

object HealthService {
  def apply(
      name: String,
      logger: TracedLogger,
      timeouts: ProcessingTimeout,
      criticalDependencies: Seq[HealthQuasiComponent] = Seq.empty,
      softDependencies: Seq[HealthQuasiComponent] = Seq.empty,
  ): HealthService =
    new HealthService(name, logger, timeouts, criticalDependencies, softDependencies)

  implicit val prettyServiceHealth: Pretty[HealthService] = prettyOfClass(
    param("name", _.name.unquoted),
    param("state", _.getState),
  )
}
