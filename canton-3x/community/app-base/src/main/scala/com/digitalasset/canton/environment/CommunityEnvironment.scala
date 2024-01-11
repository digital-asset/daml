// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.admin.api.client.data.CommunityCantonStatus
import com.digitalasset.canton.config.{CantonCommunityConfig, TestingConfigInternal}
import com.digitalasset.canton.console.{
  CantonHealthAdministration,
  CommunityCantonHealthAdministration,
  CommunityHealthDumpGenerator,
  CommunityLocalDomainReference,
  CommunityRemoteDomainReference,
  ConsoleEnvironment,
  ConsoleEnvironmentBinding,
  ConsoleGrpcAdminCommandRunner,
  ConsoleOutput,
  DomainReference,
  FeatureFlag,
  GrpcAdminCommandRunner,
  HealthDumpGenerator,
  Help,
  LocalDomainReference,
  LocalInstanceReferenceCommon,
  LocalMediatorReferenceX,
  LocalParticipantReference,
  LocalSequencerNodeReferenceX,
  NodeReferences,
  StandardConsoleOutput,
}
import com.digitalasset.canton.crypto.CommunityCryptoFactory
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService.CommunityGrpcVaultServiceFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.domain.DomainNodeBootstrap
import com.digitalasset.canton.domain.admin.v0.EnterpriseSequencerAdministrationServiceGrpc
import com.digitalasset.canton.domain.mediator.*
import com.digitalasset.canton.domain.metrics.MediatorNodeMetrics
import com.digitalasset.canton.domain.sequencing.SequencerNodeBootstrapX
import com.digitalasset.canton.domain.sequencing.config.CommunitySequencerNodeXConfig
import com.digitalasset.canton.domain.sequencing.sequencer.CommunitySequencerFactory
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.participant.{ParticipantNodeBootstrap, ParticipantNodeBootstrapX}
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
  override protected val participantNodeFactoryX
      : ParticipantNodeBootstrap.Factory[Config#ParticipantConfigType, ParticipantNodeBootstrapX] =
    ParticipantNodeBootstrapX.CommunityParticipantFactory

  override protected val domainFactory: DomainNodeBootstrap.Factory[Config#DomainConfigType] =
    DomainNodeBootstrap.CommunityDomainFactory
  override type Console = CommunityConsoleEnvironment

  override protected def _createConsole(
      consoleOutput: ConsoleOutput,
      createAdminCommandRunner: ConsoleEnvironment => ConsoleGrpcAdminCommandRunner,
  ): CommunityConsoleEnvironment =
    new CommunityConsoleEnvironment(this, consoleOutput, createAdminCommandRunner)

  override protected lazy val migrationsFactory: DbMigrationsFactory =
    new CommunityDbMigrationsFactory(loggerFactory)

  override def isEnterprise: Boolean = false

  def createHealthDumpGenerator(
      commandRunner: GrpcAdminCommandRunner
  ): HealthDumpGenerator[CommunityCantonStatus] = {
    new CommunityHealthDumpGenerator(this, commandRunner)
  }

  override protected def createSequencerX(
      name: String,
      sequencerConfig: CommunitySequencerNodeXConfig,
  ): SequencerNodeBootstrapX = {
    val nodeFactoryArguments = NodeFactoryArguments(
      name,
      sequencerConfig,
      config.sequencerNodeParametersByStringX(name),
      createClock(Some(SequencerNodeBootstrapX.LoggerFactoryKeyName -> name)),
      metricsFactory.forSequencer(name),
      testingConfig,
      futureSupervisor,
      loggerFactory.append(SequencerNodeBootstrapX.LoggerFactoryKeyName, name),
      writeHealthDumpToFile,
      configuredOpenTelemetry,
    )

    val boostrapCommonArguments = nodeFactoryArguments
      .toCantonNodeBootstrapCommonArguments(
        new CommunityStorageFactory(sequencerConfig.storage),
        new CommunityCryptoFactory(),
        new CommunityCryptoPrivateStoreFactory(),
        new CommunityGrpcVaultServiceFactory,
      )
      .valueOr(err =>
        throw new RuntimeException(s"Failed to create sequencer-x node $name: $err")
      ) // TODO(i3168): Handle node startup errors gracefully

    new SequencerNodeBootstrapX(
      boostrapCommonArguments,
      CommunitySequencerFactory,
      (_, _) =>
        StaticGrpcServices
          .notSupportedByCommunity(EnterpriseSequencerAdministrationServiceGrpc.SERVICE, logger)
          .some,
    )
  }

  override protected def createMediatorX(
      name: String,
      mediatorConfig: CommunityMediatorNodeXConfig,
  ): MediatorNodeBootstrapX = {

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
      MediatorNodeMetrics,
    ]

    new MediatorNodeBootstrapX(
      arguments,
      new CommunityMediatorReplicaManager(
        config.parameters.timeouts.processing,
        loggerFactory,
      ),
      CommunityMediatorRuntimeFactory,
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
    protected val createAdminCommandRunner: ConsoleEnvironment => ConsoleGrpcAdminCommandRunner =
      new ConsoleGrpcAdminCommandRunner(_),
) extends ConsoleEnvironment {
  override type Env = CommunityEnvironment
  override type DomainLocalRef = CommunityLocalDomainReference
  override type DomainRemoteRef = CommunityRemoteDomainReference
  override type Status = CommunityCantonStatus

  private lazy val health_ = new CommunityCantonHealthAdministration(this)
  override protected val consoleEnvironmentBindings = new ConsoleEnvironmentBinding()

  @Help.Summary("Environment health inspection")
  @Help.Group("Health")
  override def health: CantonHealthAdministration[Status] =
    health_

  override def startupOrderPrecedence(instance: LocalInstanceReferenceCommon): Int =
    instance match {
      case _: LocalSequencerNodeReferenceX => 1
      case _: LocalDomainReference => 2
      case _: LocalMediatorReferenceX => 3
      case _: LocalParticipantReference => 4
      case _ => 5
    }

  override protected def createDomainReference(name: String): DomainLocalRef =
    new CommunityLocalDomainReference(this, name, environment.executionContext)

  override protected def createRemoteDomainReference(name: String): DomainRemoteRef =
    new CommunityRemoteDomainReference(this, name)

  override protected def domainsTopLevelValue(
      h: TopLevelValue.Partial,
      domains: NodeReferences[
        DomainReference,
        CommunityRemoteDomainReference,
        CommunityLocalDomainReference,
      ],
  ): TopLevelValue[
    NodeReferences[DomainReference, CommunityRemoteDomainReference, CommunityLocalDomainReference]
  ] =
    h(domains)

  override protected def localDomainTopLevelValue(
      h: TopLevelValue.Partial,
      d: CommunityLocalDomainReference,
  ): TopLevelValue[CommunityLocalDomainReference] =
    h(d)

  override protected def remoteDomainTopLevelValue(
      h: TopLevelValue.Partial,
      d: CommunityRemoteDomainReference,
  ): TopLevelValue[CommunityRemoteDomainReference] =
    h(d)

  override protected def localDomainHelpItems(
      scope: Set[FeatureFlag],
      localDomain: CommunityLocalDomainReference,
  ): Seq[Help.Item] =
    Help.getItems(localDomain, baseTopic = Seq("$domain"), scope = scope)

  override protected def remoteDomainHelpItems(
      scope: Set[FeatureFlag],
      remoteDomain: CommunityRemoteDomainReference,
  ): Seq[Help.Item] =
    Help.getItems(remoteDomain, baseTopic = Seq("$domain"), scope = scope)
}
