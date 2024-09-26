// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.syntax.either.*
import com.digitalasset.canton.admin.api.client.data.CommunityCantonStatus
import com.digitalasset.canton.config.{CantonCommunityConfig, TestingConfigInternal}
import com.digitalasset.canton.console.{
  CantonHealthAdministration,
  CommunityCantonHealthAdministration,
  CommunityHealthDumpGenerator,
  ConsoleEnvironment,
  ConsoleEnvironmentBinding,
  ConsoleOutput,
  GrpcAdminCommandRunner,
  HealthDumpGenerator,
  Help,
  LocalInstanceReference,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerReference,
  StandardConsoleOutput,
}
import com.digitalasset.canton.crypto.CommunityCryptoFactory
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService.CommunityGrpcVaultServiceFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.domain.mediator.*
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.domain.sequencing.SequencerNodeBootstrap
import com.digitalasset.canton.domain.sequencing.config.CommunitySequencerNodeConfig
import com.digitalasset.canton.domain.sequencing.sequencer.CommunitySequencerFactory
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeBootstrap
import com.digitalasset.canton.resource.{
  CommunityDbMigrationsFactory,
  CommunityStorageFactory,
  DbMigrationsFactory,
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

  def createHealthDumpGenerator(
      commandRunner: GrpcAdminCommandRunner
  ): HealthDumpGenerator[CommunityCantonStatus] =
    new CommunityHealthDumpGenerator(this, commandRunner)

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
    )

    val bootstrapCommonArguments = nodeFactoryArguments
      .toCantonNodeBootstrapCommonArguments(
        new CommunityStorageFactory(sequencerConfig.storage),
        new CommunityCryptoFactory(),
        new CommunityCryptoPrivateStoreFactory(),
        new CommunityGrpcVaultServiceFactory,
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
        new CommunityCryptoFactory(),
        new CommunityCryptoPrivateStoreFactory(),
        new CommunityGrpcVaultServiceFactory(),
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
  override type Status = CommunityCantonStatus

  private lazy val health_ = new CommunityCantonHealthAdministration(this)
  override protected val consoleEnvironmentBindings = new ConsoleEnvironmentBinding()

  @Help.Summary("Environment health inspection")
  @Help.Group("Health")
  override def health: CantonHealthAdministration[Status] =
    health_

  override def startupOrderPrecedence(instance: LocalInstanceReference): Int =
    instance match {
      case _: LocalSequencerReference => 1
      case _: LocalMediatorReference => 2
      case _: LocalParticipantReference => 3
      case _ => 4
    }
}
