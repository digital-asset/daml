// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.data.Validated
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ConfigErrors.CantonConfigError
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.config.{
  CommunityParticipantConfig,
  RemoteParticipantConfig,
}
import com.digitalasset.canton.synchronizer.config.PublicServerConfig
import com.digitalasset.canton.synchronizer.mediator.{
  CommunityMediatorNodeConfig,
  RemoteMediatorConfig,
}
import com.digitalasset.canton.synchronizer.sequencing.config.{
  CommunitySequencerNodeConfig,
  RemoteSequencerConfig,
}
import com.digitalasset.canton.tracing.TraceContext
import com.typesafe.config.{Config, ConfigValue}
import monocle.macros.syntax.lens.*
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.{ConfigReader, ConfigWriter}

import java.io.File
import scala.annotation.nowarn

final case class CantonCommunityConfig(
    participants: Map[InstanceName, CommunityParticipantConfig] = Map.empty,
    sequencers: Map[InstanceName, CommunitySequencerNodeConfig] = Map.empty,
    mediators: Map[InstanceName, CommunityMediatorNodeConfig] = Map.empty,
    remoteParticipants: Map[InstanceName, RemoteParticipantConfig] = Map.empty,
    remoteSequencers: Map[InstanceName, RemoteSequencerConfig] = Map.empty,
    remoteMediators: Map[InstanceName, RemoteMediatorConfig] = Map.empty,
    monitoring: MonitoringConfig = MonitoringConfig(),
    parameters: CantonParameters = CantonParameters(),
    features: CantonFeatures = CantonFeatures(),
) extends CantonConfig
    with ConfigDefaults[DefaultPorts, CantonCommunityConfig] {

  override type ParticipantConfigType = CommunityParticipantConfig
  override type MediatorNodeConfigType = CommunityMediatorNodeConfig
  override type SequencerNodeConfigType = CommunitySequencerNodeConfig

  /** renders the config as json (used for dumping config for diagnostic purposes) */
  override def dumpString: String = CantonCommunityConfig.makeConfidentialString(this)

  override def validate: Validated[NonEmpty[Seq[String]], Unit] =
    CommunityConfigValidations.validate(this)

  override def withDefaults(ports: DefaultPorts): CantonCommunityConfig =
    this
      .focus(_.participants)
      .modify(_.fmap(_.withDefaults(ports)))
      .focus(_.sequencers)
      .modify(_.fmap(_.withDefaults(ports)))
      .focus(_.mediators)
      .modify(_.fmap(_.withDefaults(ports)))
}

object CantonCommunityConfig {

  private val logger: Logger = LoggerFactory.getLogger(classOf[CantonCommunityConfig])
  private val elc = ErrorLoggingContext(
    TracedLogger(logger),
    NamedLoggerFactory.root.properties,
    TraceContext.empty,
  )
  import pureconfig.generic.semiauto.*
  import CantonConfig.*

  // Implemented as a def so we can pass the ErrorLoggingContext to be used during parsing
  @nowarn("cat=unused")
  private implicit val cantonCommunityConfigReader: ConfigReader[CantonCommunityConfig] = {
    import ConfigReaders.*
    import DeprecatedConfigUtils.*
    import CantonConfig.ConfigReaders.Crypto.*
    import BaseCantonConfig.Readers.*
    implicit val driverKmsConfigReader: ConfigReader[CommunityKmsConfig.Driver] =
      deriveReader[CommunityKmsConfig.Driver]
    implicit val kmsConfigReader: ConfigReader[CommunityKmsConfig] =
      deriveReader[CommunityKmsConfig]
    implicit val communityCryptoReader: ConfigReader[CommunityCryptoConfig] =
      deriveReader[CommunityCryptoConfig]
    implicit val memoryReader: ConfigReader[StorageConfig.Memory] =
      deriveReader[StorageConfig.Memory]
    implicit val h2Reader: ConfigReader[DbConfig.H2] =
      deriveReader[DbConfig.H2]
    implicit val postgresReader: ConfigReader[DbConfig.Postgres] =
      deriveReader[DbConfig.Postgres]
    implicit val dbConfigReader: ConfigReader[DbConfig] =
      deriveReader[DbConfig]
    implicit val communityStorageConfigReader: ConfigReader[StorageConfig] =
      deriveReader[StorageConfig]
    implicit val communityParticipantConfigReader: ConfigReader[CommunityParticipantConfig] =
      deriveReader[CommunityParticipantConfig]

    implicit val communitySequencerNodeConfigReader: ConfigReader[CommunitySequencerNodeConfig] = {
      implicit val communityPublicServerConfigReader: ConfigReader[PublicServerConfig] =
        deriveReader[PublicServerConfig]
      deriveReader[CommunitySequencerNodeConfig]
    }
    implicit val communityMediatorNodeConfigReader: ConfigReader[CommunityMediatorNodeConfig] =
      deriveReader[CommunityMediatorNodeConfig]
    deriveReader[CantonCommunityConfig]
  }

  private lazy implicit val cantonCommunityConfigWriter: ConfigWriter[CantonCommunityConfig] = {
    val writers = new CantonConfig.ConfigWriters(confidential = true)
    import writers.*
    import writers.Crypto.*
    import BaseCantonConfig.Writers.*
    implicit val driverKmsConfigWriter: ConfigWriter[CommunityKmsConfig.Driver] =
      ConfigWriter.fromFunction { driverConfig =>
        implicit val driverConfigWriter: ConfigWriter[ConfigValue] =
          Crypto.driverConfigWriter(driverConfig)
        deriveWriter[CommunityKmsConfig.Driver].to(driverConfig)
      }
    implicit val kmsConfigWriter: ConfigWriter[CommunityKmsConfig] =
      deriveWriter[CommunityKmsConfig]
    implicit val communityCryptoWriter: ConfigWriter[CommunityCryptoConfig] =
      deriveWriter[CommunityCryptoConfig]
    implicit val memoryWriter: ConfigWriter[StorageConfig.Memory] =
      deriveWriter[StorageConfig.Memory]
    implicit val h2Writer: ConfigWriter[DbConfig.H2] =
      confidentialWriter[DbConfig.H2](x => x.copy(config = DbConfig.hideConfidential(x.config)))
    implicit val postgresWriter: ConfigWriter[DbConfig.Postgres] =
      confidentialWriter[DbConfig.Postgres](x =>
        x.copy(config = DbConfig.hideConfidential(x.config))
      )

    implicit val communityStorageConfigWriter: ConfigWriter[StorageConfig] =
      deriveWriter[StorageConfig]
    implicit val communityParticipantConfigWriter: ConfigWriter[CommunityParticipantConfig] =
      deriveWriter[CommunityParticipantConfig]
    implicit val communitySequencerNodeConfigWriter: ConfigWriter[CommunitySequencerNodeConfig] = {
      implicit val communityPublicServerConfigWriter: ConfigWriter[PublicServerConfig] =
        deriveWriter[PublicServerConfig]
      deriveWriter[CommunitySequencerNodeConfig]
    }
    implicit val communityMediatorNodeConfigWriter: ConfigWriter[CommunityMediatorNodeConfig] =
      deriveWriter[CommunityMediatorNodeConfig]

    deriveWriter[CantonCommunityConfig]
  }

  def load(config: Config)(implicit
      elc: ErrorLoggingContext = elc
  ): Either[CantonConfigError, CantonCommunityConfig] =
    CantonConfig.loadAndValidate[CantonCommunityConfig](config)

  def loadOrExit(config: Config)(implicit elc: ErrorLoggingContext = elc): CantonCommunityConfig =
    CantonConfig.loadOrExit[CantonCommunityConfig](config)

  def parseAndLoad(files: Seq[File])(implicit
      elc: ErrorLoggingContext = elc
  ): Either[CantonConfigError, CantonCommunityConfig] =
    CantonConfig.parseAndLoad[CantonCommunityConfig](files)

  def parseAndLoadOrExit(files: Seq[File])(implicit
      elc: ErrorLoggingContext = elc
  ): CantonCommunityConfig =
    CantonConfig.parseAndLoadOrExit[CantonCommunityConfig](files)

  def makeConfidentialString(config: CantonCommunityConfig): String =
    "canton " + ConfigWriter[CantonCommunityConfig]
      .to(config)
      .render(CantonConfig.defaultConfigRenderer)

}
