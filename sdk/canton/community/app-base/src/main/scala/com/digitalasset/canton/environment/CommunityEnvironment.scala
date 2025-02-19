// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.syntax.either.*
import com.digitalasset.canton.config.{CantonCommunityConfig, TestingConfigInternal}
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleEnvironmentBinding,
  ConsoleOutput,
  StandardConsoleOutput,
}
import com.digitalasset.canton.crypto.kms.CommunityKmsFactory
import com.digitalasset.canton.crypto.store.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeBootstrap
import com.digitalasset.canton.resource.{
  CommunityDbMigrationsFactory,
  CommunityStorageFactory,
  DbMigrationsFactory,
}
import com.digitalasset.canton.synchronizer.mediator.{
  CommunityMediatorNodeConfig,
  CommunityMediatorReplicaManager,
  MediatorNodeBootstrap,
  MediatorNodeConfigCommon,
  MediatorNodeParameters,
}
import com.digitalasset.canton.synchronizer.metrics.MediatorMetrics
import com.digitalasset.canton.synchronizer.sequencer.config.CommunitySequencerNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.{
  CommunitySequencerFactory,
  SequencerNodeBootstrap,
}

class CommunityEnvironment(
    override val config: CantonCommunityConfig,
    override val testingConfig: TestingConfigInternal,
    override val loggerFactory: NamedLoggerFactory,
) extends Environment {

  override type Config = CantonCommunityConfig

  override protected val participantNodeFactory
      : ParticipantNodeBootstrap.Factory[Config#ParticipantConfigType, ParticipantNodeBootstrap] =
    ParticipantNodeBootstrap.CommunityParticipantFactory

  override type Console = CommunityConsoleEnvironment

  override protected def _createConsole(
      consoleOutput: ConsoleOutput
  ): CommunityConsoleEnvironment =
    new CommunityConsoleEnvironment(this, consoleOutput)

  override protected lazy val migrationsFactory: DbMigrationsFactory =
    new CommunityDbMigrationsFactory(loggerFactory)

  override def isEnterprise: Boolean = false

  override protected def createSequencer(
      name: String,
      sequencerConfig: CommunitySequencerNodeConfig,
  ): SequencerNodeBootstrap = {
    val nodeFactoryArguments = NodeFactoryArguments(
      name,
      sequencerConfig,
      config.sequencerNodeParametersByString(name),
      createClock(Some(SequencerNodeBootstrap.LoggerFactoryKeyName -> name)),
      metricsRegistry.forSequencer(name),
      testingConfig,
      futureSupervisor,
      loggerFactory.append(SequencerNodeBootstrap.LoggerFactoryKeyName, name),
      writeHealthDumpToFile,
      configuredOpenTelemetry,
      executionContext,
    )

    val bootstrapCommonArguments = nodeFactoryArguments
      .toCantonNodeBootstrapCommonArguments(
        new CommunityStorageFactory(sequencerConfig.storage),
        new CommunityCryptoPrivateStoreFactory(
          nodeFactoryArguments.config.crypto.provider,
          nodeFactoryArguments.config.crypto.kms,
          CommunityKmsFactory,
          nodeFactoryArguments.config.parameters.caching.kmsMetadataCache,
          nodeFactoryArguments.config.crypto.privateKeyStore,
          nodeFactoryArguments.parameters.nonStandardConfig,
          nodeFactoryArguments.futureSupervisor,
          nodeFactoryArguments.clock,
          nodeFactoryArguments.executionContext,
        ),
        CommunityKmsFactory,
      )
      .valueOr(err =>
        throw new RuntimeException(s"Failed to create sequencer node $name: $err")
      ) // TODO(i3168): Handle node startup errors gracefully

    new SequencerNodeBootstrap(bootstrapCommonArguments, CommunitySequencerFactory)
  }

  override protected def createMediator(
      name: String,
      mediatorConfig: CommunityMediatorNodeConfig,
  ): MediatorNodeBootstrap = {

    val factoryArguments = mediatorNodeFactoryArguments(name, mediatorConfig)
    val arguments = factoryArguments
      .toCantonNodeBootstrapCommonArguments(
        new CommunityStorageFactory(mediatorConfig.storage),
        new CommunityCryptoPrivateStoreFactory(
          factoryArguments.config.crypto.provider,
          factoryArguments.config.crypto.kms,
          CommunityKmsFactory,
          factoryArguments.config.parameters.caching.kmsMetadataCache,
          factoryArguments.config.crypto.privateKeyStore,
          factoryArguments.parameters.nonStandardConfig,
          factoryArguments.futureSupervisor,
          factoryArguments.clock,
          factoryArguments.executionContext,
        ),
        CommunityKmsFactory,
      )
      .valueOr(err =>
        throw new RuntimeException(s"Failed to create mediator bootstrap: $err")
      ): CantonNodeBootstrapCommonArguments[
      MediatorNodeConfigCommon,
      MediatorNodeParameters,
      MediatorMetrics,
    ]

    new MediatorNodeBootstrap(
      arguments,
      new CommunityMediatorReplicaManager(
        config.parameters.timeouts.processing,
        loggerFactory,
      ),
    )
  }
}

object CommunityEnvironmentFactory extends EnvironmentFactory[CommunityEnvironment] {
  override def create(
      config: CantonCommunityConfig,
      loggerFactory: NamedLoggerFactory,
      testingConfigInternal: TestingConfigInternal,
  ): CommunityEnvironment =
    new CommunityEnvironment(config, testingConfigInternal, loggerFactory)
}

class CommunityConsoleEnvironment(
    val environment: CommunityEnvironment,
    val consoleOutput: ConsoleOutput = StandardConsoleOutput,
) extends ConsoleEnvironment {
  override type Env = CommunityEnvironment

  override protected val consoleEnvironmentBindings = new ConsoleEnvironmentBinding()
}
