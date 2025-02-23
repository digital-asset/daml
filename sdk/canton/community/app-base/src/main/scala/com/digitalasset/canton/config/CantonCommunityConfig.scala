// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.data.Validated
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ConfigErrors.CantonConfigError
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.config.{LocalParticipantConfig, RemoteParticipantConfig}
import com.digitalasset.canton.synchronizer.config.PublicServerConfig
import com.digitalasset.canton.synchronizer.mediator.{MediatorNodeConfig, RemoteMediatorConfig}
import com.digitalasset.canton.synchronizer.sequencer.config.{
  CommunitySequencerNodeConfig,
  RemoteSequencerConfig,
}
import com.digitalasset.canton.tracing.TraceContext
import com.typesafe.config.Config
import monocle.macros.syntax.lens.*
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.{ConfigReader, ConfigWriter}

import java.io.File
import scala.annotation.nowarn

final case class CantonCommunityConfig(
    participants: Map[InstanceName, LocalParticipantConfig] = Map.empty,
    sequencers: Map[InstanceName, CommunitySequencerNodeConfig] = Map.empty,
    mediators: Map[InstanceName, MediatorNodeConfig] = Map.empty,
    remoteParticipants: Map[InstanceName, RemoteParticipantConfig] = Map.empty,
    remoteSequencers: Map[InstanceName, RemoteSequencerConfig] = Map.empty,
    remoteMediators: Map[InstanceName, RemoteMediatorConfig] = Map.empty,
    monitoring: MonitoringConfig = MonitoringConfig(),
    parameters: CantonParameters = CantonParameters(),
    features: CantonFeatures = CantonFeatures(),
) extends CantonConfig
    with ConfigDefaults[DefaultPorts, CantonCommunityConfig]
    with CommunityOnlyCantonConfigValidation {

  override type SequencerNodeConfigType = CommunitySequencerNodeConfig

  /** renders the config as json (used for dumping config for diagnostic purposes) */
  override def dumpString: String = CantonCommunityConfig.makeConfidentialString(this)

  override def edition: CantonEdition = CommunityCantonEdition

  override def validate: Validated[NonEmpty[Seq[String]], Unit] =
    CommunityConfigValidations.validate(this, edition)

  override def withDefaults(ports: DefaultPorts, edition: CantonEdition): CantonCommunityConfig =
    this
      .focus(_.participants)
      .modify(_.fmap(_.withDefaults(ports, edition)))
      .focus(_.sequencers)
      .modify(_.fmap(_.withDefaults(ports, edition)))
      .focus(_.mediators)
      .modify(_.fmap(_.withDefaults(ports, edition)))
}

object CantonCommunityConfig {

  implicit val cantonCommunityConfigCantonConfigValidator
      : CantonConfigValidator[CantonCommunityConfig] =
    CantonConfigValidatorDerivation[CantonCommunityConfig]

  private val logger: Logger = LoggerFactory.getLogger(classOf[CantonCommunityConfig])
  private val elc = ErrorLoggingContext(
    TracedLogger(logger),
    NamedLoggerFactory.root.properties,
    TraceContext.empty,
  )
  import pureconfig.generic.semiauto.*
  import CantonConfig.*

  @nowarn("cat=unused")
  private implicit val cantonCommunityConfigReader: ConfigReader[CantonCommunityConfig] = {
    import ConfigReaders.*
    import DeprecatedConfigUtils.*
    import CantonConfig.ConfigReaders.Crypto.*
    import BaseCantonConfig.Readers.*

    implicit val communitySequencerNodeConfigReader: ConfigReader[CommunitySequencerNodeConfig] = {
      implicit val communityPublicServerConfigReader: ConfigReader[PublicServerConfig] =
        deriveReader[PublicServerConfig]
      deriveReader[CommunitySequencerNodeConfig]
    }
    deriveReader[CantonCommunityConfig]
  }

  private lazy implicit val cantonCommunityConfigWriter: ConfigWriter[CantonCommunityConfig] = {
    val writers = new CantonConfig.ConfigWriters(confidential = true)
    import writers.*
    import writers.Crypto.*

    implicit val communitySequencerNodeConfigWriter: ConfigWriter[CommunitySequencerNodeConfig] = {
      implicit val communityPublicServerConfigWriter: ConfigWriter[PublicServerConfig] =
        deriveWriter[PublicServerConfig]
      deriveWriter[CommunitySequencerNodeConfig]
    }

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
