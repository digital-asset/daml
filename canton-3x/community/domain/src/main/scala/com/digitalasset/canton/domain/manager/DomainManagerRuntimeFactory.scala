// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.manager

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.initialization.TopologyManagementComponents
import com.digitalasset.canton.domain.topology.DomainTopologyManager
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

trait DomainManagerRuntimeFactory {
  def create(
      manager: DomainTopologyManager,
      topologyManagementComponents: TopologyManagementComponents,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      writeService: ServerServiceDefinition,
      initializationService: ServerServiceDefinition,
  )(implicit ec: ExecutionContext): EitherT[Future, String, DomainManagerRuntime]
}
