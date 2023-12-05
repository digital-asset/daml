// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{LocalNodeConfig, TestingConfigInternal}
import com.digitalasset.canton.crypto.CryptoFactory
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService.GrpcVaultServiceFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CryptoPrivateStoreFactory
import com.digitalasset.canton.environment.CantonNodeBootstrap.HealthDumpFunction
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.StorageFactory
import com.digitalasset.canton.telemetry.ConfiguredOpenTelemetry
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TracerProvider

final case class NodeFactoryArguments[
    NodeConfig <: LocalNodeConfig,
    ParameterConfig <: CantonNodeParameters,
    Metrics <: BaseMetrics,
](
    name: String,
    config: NodeConfig,
    parameters: ParameterConfig,
    clock: Clock,
    metrics: Metrics,
    testingConfig: TestingConfigInternal,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
    writeHealthDumpToFile: HealthDumpFunction,
    configuredOpenTelemetry: ConfiguredOpenTelemetry,
) {
  val tracerProvider: TracerProvider = TracerProvider.Factory(configuredOpenTelemetry, name)

  def toCantonNodeBootstrapCommonArguments(
      storageFactory: StorageFactory,
      cryptoFactory: CryptoFactory,
      cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
      grpcVaultServiceFactory: GrpcVaultServiceFactory,
  ): Either[String, CantonNodeBootstrapCommonArguments[NodeConfig, ParameterConfig, Metrics]] =
    InstanceName
      .create(name)
      .map(
        CantonNodeBootstrapCommonArguments(
          _,
          config,
          parameters,
          testingConfig,
          clock,
          metrics,
          storageFactory,
          cryptoFactory,
          cryptoPrivateStoreFactory,
          grpcVaultServiceFactory,
          futureSupervisor,
          loggerFactory,
          writeHealthDumpToFile,
          configuredOpenTelemetry,
          tracerProvider,
        )
      )
      .leftMap(_.toString)
}
