// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.data.Validated
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ConfigErrors.CantonConfigError
import com.digitalasset.canton.domain.config.{
  CommunityDomainConfig,
  DomainBaseConfig,
  RemoteDomainConfig,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.config.{
  CommunityParticipantConfig,
  LocalParticipantConfig,
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
    remoteDomains: Map[InstanceName, RemoteDomainConfig] = Map.empty,
    remoteParticipants: Map[InstanceName, RemoteParticipantConfig] = Map.empty,
    monitoring: MonitoringConfig = MonitoringConfig(),
    parameters: CantonParameters = CantonParameters(),
    features: CantonFeatures = CantonFeatures(),
) extends CantonConfig
    with ConfigDefaults[DefaultPorts, CantonCommunityConfig] {

  override type DomainConfigType = CommunityDomainConfig
  override type ParticipantConfigType = CommunityParticipantConfig

  /** renders the config as json (used for dumping config for diagnostic purposes) */
  override def dumpString: String = CantonCommunityConfig.makeConfidentialString(this)

  override def validate: Validated[NonEmpty[Seq[String]], Unit] =
    CommunityConfigValidations.validate(this)(parameters)

  override def withDefaults(ports: DefaultPorts): CantonCommunityConfig =
    this
      .focus(_.domains)
      .modify(_.fmap(_.withDefaults(ports)))
      .focus(_.participants)
      .modify(_.fmap(_.withDefaults(ports)))
}

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
object CantonCommunityConfig {

  /** Combine together deprecated implicits for types that define them
    * This setup allows the compiler to pick the implicit for the most specific type when applying deprecations.
    * For instance,
    *   ConfigReader[LocalParticipantConfig].applyDeprecations will pick up the deprecations implicit defined in
    *   LocalParticipantConfig instead of LocalNodeConfig
    *   despite LocalParticipantConfig being a subtype of LocalNodeConfig.
    */
  object CantonDeprecationImplicits
      extends LocalNodeConfig.LocalNodeConfigDeprecationImplicits
      with LocalParticipantConfig.LocalParticipantDeprecationsImplicits
      with DomainBaseConfig.DomainBaseConfigDeprecationImplicits

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
  private implicit def cantonCommunityConfigReader(implicit
      elc: ErrorLoggingContext
  ): ConfigReader[CantonCommunityConfig] = { // memoize it so we get the same instance every time
    val configReaders: ConfigReaders = new ConfigReaders()
    import configReaders.*
    import DeprecatedConfigUtils.*
    import CantonDeprecationImplicits.*

    implicit val communityDomainConfigReader: ConfigReader[CommunityDomainConfig] =
      deriveReader[CommunityDomainConfig].applyDeprecations
    implicit val communityParticipantConfigReader: ConfigReader[CommunityParticipantConfig] =
      deriveReader[CommunityParticipantConfig].applyDeprecations

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
