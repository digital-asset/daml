// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.data.Validated
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.domain.config.DomainParametersConfig
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HandshakeErrors.DeprecatedProtocolVersion
import com.digitalasset.canton.version.ProtocolVersion

import java.net.URI

private[config] trait ConfigValidations[C <: CantonConfig] {
  final def validate(config: C): Validated[NonEmpty[Seq[String]], Unit] =
    validations.traverse_(_(config))

  protected val validations: List[C => Validated[NonEmpty[Seq[String]], Unit]]
}

object CommunityConfigValidations
    extends ConfigValidations[CantonCommunityConfig]
    with NamedLogging {
  import TraceContext.Implicits.Empty.*
  override protected def loggerFactory: NamedLoggerFactory = NamedLoggerFactory.root

  final case class DbAccess(url: String, user: Option[String]) {
    private lazy val urlNoPassword = {
      val uri = new URI(
        url.replace("jdbc:", "")
      )
      val queryNoPassword = Option(uri.getQuery)
        .getOrElse("")
        .split('&')
        .map(param =>
          if (param.startsWith("password=")) ""
          else param
        )
        .mkString
      new URI(uri.getScheme, uri.getAuthority, uri.getPath, queryNoPassword, uri.getFragment)
    }

    override def toString: String =
      s"DbAccess($urlNoPassword, $user)"
  }

  private val Valid: Validated[NonEmpty[Seq[String]], Unit] = Validated.valid(())
  type Validation = CantonCommunityConfig => Validated[NonEmpty[Seq[String]], Unit]

  override protected val validations: List[Validation] =
    List[Validation](noDuplicateStorage, atLeastOneNode) ++ genericValidations[
      CantonCommunityConfig
    ]

  /** Validations applied to all community and enterprise Canton configurations. */
  private[config] def genericValidations[C <: CantonConfig]
      : List[C => Validated[NonEmpty[Seq[String]], Unit]] =
    List(
      backwardsCompatibleLoggingConfig,
      developmentProtocolSafetyCheckDomains,
      developmentProtocolSafetyCheckParticipants,
      warnIfUnsafeMinProtocolVersion,
      warnIfUnsafeProtocolVersionEmbeddedDomain,
      adminTokenSafetyCheckParticipants,
    )

  /** Group node configs by db access to find matching db storage configs.
    * Overcomplicated types used are to work around that at this point nodes could have conflicting names so we can't just
    * throw them all in a single map.
    */
  private[config] def extractNormalizedDbAccess[C <: CantonConfig](
      nodeConfigs: Map[String, LocalNodeConfig]*
  ): Map[DbAccess, List[(String, LocalNodeConfig)]] = {
    // Basic attempt to normalize JDBC URL-based configuration and explicit property configuration
    // Limitations: Does not parse nor normalize the JDBC URLs
    def normalize(dbConfig: DbConfig): Option[DbAccess] = {
      import slick.util.ConfigExtensionMethods.*

      val slickConfig = dbConfig.config

      def getPropStr(prop: String): Option[String] =
        slickConfig.getStringOpt(prop).orElse(slickConfig.getStringOpt(s"properties.$prop"))

      def getPropInt(prop: String): Option[Int] =
        slickConfig.getIntOpt(prop).orElse(slickConfig.getIntOpt(s"properties.$prop"))

      def extractUrl: Option[String] =
        getPropStr("url").orElse(getPropStr("jdbcUrl"))

      def extractServerPortDbAsUrl: Option[String] =
        for {
          server <- getPropStr("serverName")
          port <- getPropInt("portNumber")
          dbName <- getPropStr("databaseName")
          url = dbConfig match {
            case _: H2DbConfig => DbConfig.h2Url(dbName)
            case _: PostgresDbConfig => DbConfig.postgresUrl(server, port, dbName)
            // Assume Oracle
            case _ => DbConfig.oracleUrl(server, port, dbName)
          }
        } yield url

      val user = getPropStr("user")
      extractUrl.orElse(extractServerPortDbAsUrl).map(url => DbAccess(url = url, user = user))
    }

    // combine into a single list of name to config
    val configs = nodeConfigs.map(_.toList).foldLeft(List[(String, LocalNodeConfig)]())(_ ++ _)

    val withStorageConfigs = configs.mapFilter { case (name, config) =>
      config.storage match {
        case dbConfig: DbConfig => normalize(dbConfig).map((_, name, config))
        case _ => None
      }
    }

    withStorageConfigs
      .groupBy { case (dbAccess, _, _) => dbAccess }
      .fmap(_.map { case (_, name, config) =>
        (name, config)
      })
  }

  private[config] def formatNodeList(nodes: List[(String, LocalNodeConfig)]): String =
    nodes.map { case (name, config) => s"${config.nodeTypeName} $name" }.mkString(",")

  /** Validate the config that the storage configuration is not shared between nodes. */
  private def noDuplicateStorage(
      config: CantonCommunityConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val dbAccessToNodes =
      extractNormalizedDbAccess(config.participantsByString, config.domainsByString)

    dbAccessToNodes.toSeq
      .traverse_ {
        case (dbAccess, nodes) if nodes.lengthCompare(1) > 0 =>
          Validated.invalid(
            NonEmpty(Seq, s"Nodes ${formatNodeList(nodes)} share same DB access: $dbAccess")
          )
        case _ => Validated.valid(())
      }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private def atLeastOneNode(
      config: CantonCommunityConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val CantonCommunityConfig(
      domains,
      participants,
      participantsX,
      remoteDomains,
      remoteParticipants,
      remoteParticipantsX,
      _,
      _,
      _,
    ) =
      config
    Validated.cond(
      Seq(
        domains,
        participants,
        remoteDomains,
        remoteParticipants,
        participantsX,
        remoteParticipantsX,
      )
        .exists(_.nonEmpty),
      (),
      NonEmpty(Seq, "At least one node must be defined in the configuration"),
    )

  }

  /** Check that logging configs are backwards compatible but consistent */
  private def backwardsCompatibleLoggingConfig(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    (config.monitoring.logMessagePayloads, config.monitoring.logging.api.messagePayloads) match {
      case (Some(fst), Some(snd)) =>
        Validated.cond(
          fst == snd,
          (),
          NonEmpty(Seq, backwardsCompatibleLoggingConfigErr),
        )
      case _ => Valid
    }
  }

  private[config] val backwardsCompatibleLoggingConfigErr =
    "Inconsistent configuration of canton.monitoring.log-message-payloads and canton.monitoring.logging.api.message-payloads. Please use the latter in your configuration"

  private def developmentProtocolSafetyCheckDomains(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    developmentProtocolSafetyCheck(
      config.parameters.nonStandardConfig,
      config.domains.toSeq.map { case (k, v) =>
        (k, v.init.domainParameters)
      },
    )
  }

  private def developmentProtocolSafetyCheckParticipants(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    def toNe(
        name: String,
        nonStandardConfig: Boolean,
        devVersionSupport: Boolean,
    ): Validated[NonEmpty[Seq[String]], Unit] = {
      Validated.cond(
        nonStandardConfig || !devVersionSupport,
        (),
        NonEmpty(
          Seq,
          s"Enabling dev-version-support for participant ${name} requires you to explicitly set canton.parameters.non-standard-config = yes",
        ),
      )
    }

    config.participants.toList.traverse_ { case (name, participantConfig) =>
      toNe(
        name.unwrap,
        config.parameters.nonStandardConfig,
        participantConfig.parameters.devVersionSupport,
      )
    }
  }

  private def warnIfUnsafeMinProtocolVersion(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    config.participants.toSeq.foreach { case (name, config) =>
      val minimum = config.parameters.minimumProtocolVersion.map(_.unwrap)
      val isMinimumDeprecatedVersion = minimum.getOrElse(ProtocolVersion.minimum).isDeprecated

      if (isMinimumDeprecatedVersion && !config.parameters.dontWarnOnDeprecatedPV)
        DeprecatedProtocolVersion.WarnParticipant(name, minimum).discard
    }
    Validated.valid(())
  }

  private def warnIfUnsafeProtocolVersionEmbeddedDomain(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    config.domains.toSeq.foreach { case (name, config) =>
      val pv = config.init.domainParameters.protocolVersion.unwrap
      if (pv.isDeprecated && !config.init.domainParameters.dontWarnOnDeprecatedPV)
        DeprecatedProtocolVersion.WarnDomain(name, pv).discard
    }
    Validated.valid(())
  }

  private[config] def developmentProtocolSafetyCheck(
      allowUnstableProtocolVersion: Boolean,
      namesAndConfig: Seq[(InstanceName, DomainParametersConfig)],
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    def toNe(
        name: String,
        protocolVersion: ProtocolVersion,
        allowUnstableProtocolVersion: Boolean,
    ): Validated[NonEmpty[Seq[String]], Unit] = {
      Validated.cond(
        protocolVersion.isStable || allowUnstableProtocolVersion,
        (),
        NonEmpty(
          Seq,
          s"Using non-stable protocol ${protocolVersion} for node ${name} requires you to explicitly set canton.parameters.non-standard-config = yes",
        ),
      )
    }

    namesAndConfig.toList.traverse_ { case (name, parameters) =>
      toNe(
        name.unwrap,
        parameters.protocolVersion.version,
        allowUnstableProtocolVersion,
      )
    }

  }

  private def adminTokenSafetyCheckParticipants(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    def toNe(
        name: String,
        nonStandardConfig: Boolean,
        adminToken: Option[String],
    ): Validated[NonEmpty[Seq[String]], Unit] = {
      Validated.cond(
        nonStandardConfig || adminToken.isEmpty,
        (),
        NonEmpty(
          Seq,
          s"Setting ledger-api.admin-token for participant ${name} requires you to explicitly set canton.parameters.non-standard-config = yes",
        ),
      )
    }

    config.participants.toList.traverse_ { case (name, participantConfig) =>
      toNe(
        name.unwrap,
        config.parameters.nonStandardConfig,
        participantConfig.ledgerApi.adminToken,
      )
    }
  }

}
