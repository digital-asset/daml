// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.data.Validated
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ConfigErrors.CantonConfigError
import com.digitalasset.canton.domain.config.{CommunityDomainConfig, RemoteDomainConfig}
import com.digitalasset.canton.domain.mediator.{CommunityMediatorNodeXConfig, RemoteMediatorConfig}
import com.digitalasset.canton.domain.sequencing.config.{
  CommunitySequencerNodeXConfig,
  RemoteSequencerConfig,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.config.{
  CommunityParticipantConfig,
  RemoteParticipantConfig,
}
import com.digitalasset.canton.tracing.TraceContext
import com.typesafe.config.Config
import monocle.macros.syntax.lens.*
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.{ConfigReader, ConfigWriter}

import java.io.File
import scala.annotation.nowarn

final case class CantonCommunityConfig(
    domains: Map[InstanceName, CommunityDomainConfig] = Map.empty,
    participants: Map[InstanceName, CommunityParticipantConfig] = Map.empty,
    participantsX: Map[InstanceName, CommunityParticipantConfig] = Map.empty,
    sequencersX: Map[InstanceName, CommunitySequencerNodeXConfig] = Map.empty,
    mediatorsX: Map[InstanceName, CommunityMediatorNodeXConfig] = Map.empty,
    remoteDomains: Map[InstanceName, RemoteDomainConfig] = Map.empty,
    remoteParticipants: Map[InstanceName, RemoteParticipantConfig] = Map.empty,
    remoteParticipantsX: Map[InstanceName, RemoteParticipantConfig] = Map.empty,
    remoteSequencersX: Map[InstanceName, RemoteSequencerConfig] = Map.empty,
    remoteMediatorsX: Map[InstanceName, RemoteMediatorConfig] = Map.empty,
    monitoring: MonitoringConfig = MonitoringConfig(),
    parameters: CantonParameters = CantonParameters(),
    features: CantonFeatures = CantonFeatures(),
) extends CantonConfig
    with ConfigDefaults[DefaultPorts, CantonCommunityConfig] {

  override type DomainConfigType = CommunityDomainConfig
  override type ParticipantConfigType = CommunityParticipantConfig
  override type MediatorNodeXConfigType = CommunityMediatorNodeXConfig
  override type SequencerNodeXConfigType = CommunitySequencerNodeXConfig

  /** renders the config as json (used for dumping config for diagnostic purposes) */
  override def dumpString: String = CantonCommunityConfig.makeConfidentialString(this)

  override def validate: Validated[NonEmpty[Seq[String]], Unit] =
    CommunityConfigValidations.validate(this)

  override def withDefaults(ports: DefaultPorts): CantonCommunityConfig =
    this
      .focus(_.domains)
      .modify(_.fmap(_.withDefaults(ports)))
      .focus(_.participants)
      .modify(_.fmap(_.withDefaults(ports)))
      .focus(_.participantsX)
      .modify(_.fmap(_.withDefaults(ports)))
}

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
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

    implicit val communityDomainConfigReader: ConfigReader[CommunityDomainConfig] =
      deriveReader[CommunityDomainConfig]
    implicit val communityParticipantConfigReader: ConfigReader[CommunityParticipantConfig] =
      deriveReader[CommunityParticipantConfig]
    implicit val communitySequencerNodeXConfigReader: ConfigReader[CommunitySequencerNodeXConfig] =
      deriveReader[CommunitySequencerNodeXConfig]
    implicit val communityMediatorNodeXConfigReader: ConfigReader[CommunityMediatorNodeXConfig] =
      deriveReader[CommunityMediatorNodeXConfig]

    deriveReader[CantonCommunityConfig]
  }

  @nowarn("cat=unused")
  private lazy implicit val cantonCommunityConfigWriter: ConfigWriter[CantonCommunityConfig] = {
    val writers = new CantonConfig.ConfigWriters(confidential = true)
    import writers.*
    implicit val communityDomainConfigWriter: ConfigWriter[CommunityDomainConfig] =
      deriveWriter[CommunityDomainConfig]
    implicit val communityParticipantConfigWriter: ConfigWriter[CommunityParticipantConfig] =
      deriveWriter[CommunityParticipantConfig]
    implicit val communitySequencerNodeXConfigWriter: ConfigWriter[CommunitySequencerNodeXConfig] =
      deriveWriter[CommunitySequencerNodeXConfig]
    implicit val communityMediatorNodeXConfigWriter: ConfigWriter[CommunityMediatorNodeXConfig] =
      deriveWriter[CommunityMediatorNodeXConfig]

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
