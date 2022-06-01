// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common.{Config, ParticipantConfig}
import com.daml.lf.data.Ref
import com.daml.lf.engine.EngineConfig
import com.daml.lf.language.LanguageVersion
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.daml.metrics.MetricsReporting
import com.daml.platform.apiserver.ApiServerConfig
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.configuration.{InitialLedgerConfiguration, PartyConfiguration}
import com.daml.platform.indexer.IndexerConfig
import com.daml.platform.sandbox.logging
import com.daml.platform.usermanagement.UserManagementConfig
import com.daml.ports.Port

import java.io.File
import java.time.Duration
import java.util.UUID
import scala.concurrent.duration._

class SandboxOnXForTest(
    genericConfig: Config,
    bridgeConfig: BridgeConfig,
    configAdaptor: BridgeConfigAdaptor,
    damlPackages: List[File],
)(implicit materializer: Materializer)
    extends ResourceOwner[Port] {

  def acquire()(implicit resourceContext: ResourceContext): Resource[Port] = {
    for {
      (participantId, dataSource, participantConfig) <- SandboxOnXRunner.combinedParticipant(
        genericConfig
      )
      metrics <- new MetricsReporting(
        classOf[SandboxOnXForTest].getName,
        None,
        10.seconds,
      ).acquire()

      (apiServer, writeService, indexService) <-
        SandboxOnXRunner
          .buildLedger(
            participantId,
            genericConfig,
            participantConfig,
            dataSource,
            bridgeConfig,
            materializer,
            materializer.system,
            configAdaptor,
            Some(metrics),
          )
          .acquire()
      _ <- newLoggingContextWith(
        logging.participantId(participantId)
      ) { implicit loggingContext =>
        new PackageUploader(writeService, indexService)
          .upload(damlPackages)
          .acquire()
      }
    } yield {
      apiServer.port
    }
  }

}

object SandboxOnXForTest {
  val SandboxEngineConfig = EngineConfig(
    allowedLanguageVersions = LanguageVersion.StableVersions,
    profileDir = None,
    stackTraceMode = true,
    forbidV0ContractId = true,
  )
  val SandboxParticipantId = Ref.ParticipantId.assertFromString("sandbox-participant")
  val SandboxParticipantConfig =
    ParticipantConfig(
      apiServer = ApiServerConfig().copy(
        initialLedgerConfiguration = Some(
          InitialLedgerConfiguration(
            maxDeduplicationDuration = Duration.ofMinutes(30L),
            avgTransactionLatency =
              Configuration.reasonableInitialConfiguration.timeModel.avgTransactionLatency,
            minSkew = Configuration.reasonableInitialConfiguration.timeModel.minSkew,
            maxSkew = Configuration.reasonableInitialConfiguration.timeModel.maxSkew,
            delayBeforeSubmitting = Duration.ZERO,
          )
        ),
        userManagement = UserManagementConfig.default(true),
        maxInboundMessageSize = 4194304,
        configurationLoadTimeout = 10000.millis,
        party = PartyConfiguration(implicitPartyAllocation = true),
        seeding = Seeding.Strong,
        port = Port(0),
        managementServiceTimeout = 120000.millis,
      ),
      indexer = IndexerConfig(
        inputMappingParallelism = 512
      ),
    )
  val SandboxDefault = Config(
    engine = SandboxEngineConfig,
    dataSource = Config.Default.dataSource.map { case _ -> value => (SandboxParticipantId, value) },
    participants = Map(SandboxParticipantId -> SandboxParticipantConfig),
  )

  class SandboxOnXForTestConfigAdaptor(authServiceOverwrite: Option[AuthService])
      extends BridgeConfigAdaptor {
    override def authService(apiServerConfig: ApiServerConfig): AuthService = {
      authServiceOverwrite.getOrElse(AuthServiceWildcard)
    }
  }

  def defaultH2SandboxJdbcUrl() =
    s"jdbc:h2:mem:sandbox-${UUID.randomUUID().toString};db_close_delay=-1"

  case class CustomConfig(
      genericConfig: Config,
      damlPackages: List[File] = List.empty,
  )

  def owner(
      config: SandboxOnXForTest.CustomConfig,
      bridgeConfig: BridgeConfig,
      authService: Option[AuthService],
  ): ResourceOwner[Port] = {
    val configAdaptor: BridgeConfigAdaptor = new SandboxOnXForTestConfigAdaptor(
      authService
    )
    for {
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem("sandbox"))
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
      server <- new SandboxOnXForTest(
        config.genericConfig,
        bridgeConfig,
        configAdaptor,
        config.damlPackages,
      )(materializer)
    } yield server
  }

}
