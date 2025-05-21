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
  final def validate(config: C)(
      parameters: CantonParameters
  ): Validated[NonEmpty[Seq[String]], Unit] =
    validations(parameters).traverse_(_(config))

  protected def validations(
      parameters: CantonParameters
  ): List[C => Validated[NonEmpty[Seq[String]], Unit]]
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

  override protected def validations(parameters: CantonParameters): List[Validation] =
    List[Validation](noDuplicateStorage, atLeastOneNode) ++
      genericValidations[CantonCommunityConfig](parameters)

  /** Validations applied to all community and enterprise Canton configurations. */
  private[config] def genericValidations[C <: CantonConfig](
      parameters: CantonParameters
  ): List[C => Validated[NonEmpty[Seq[String]], Unit]] =
    List(
      protocolVersionDefinedForDomains,
      backwardsCompatibleLoggingConfig,
      developmentProtocolSafetyCheckDomains,
      betaProtocolSafetyCheck(_)(parameters),
      developmentProtocolSafetyCheckParticipants,
      warnIfUnsafeMinProtocolVersion,
      warnIfDeprecatedProtocolVersionEmbeddedDomain,
      adminTokenSafetyCheckParticipants,
      adminTokensMatchOnParticipants,
      eitherUserListsOrPrivilegedTokensOnParticipants,
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
      remoteDomains,
      remoteParticipants,
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
      )
        .exists(_.nonEmpty),
      (),
      NonEmpty(Seq, "At least one node must be defined in the configuration"),
    )

  }

  /** Check that logging configs are backwards compatible but consistent */
  private def backwardsCompatibleLoggingConfig(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] =
    (config.monitoring.logMessagePayloads, config.monitoring.logging.api.messagePayloads) match {
      case (Some(fst), Some(snd)) =>
        Validated.cond(
          fst == snd,
          (),
          NonEmpty(Seq, backwardsCompatibleLoggingConfigErr),
        )
      case _ => Valid
    }

  // Check that the protocol version is defined for the domains
  private def protocolVersionDefinedForDomains(config: CantonConfig) = {
    val errors = config.domains.toSeq.foldLeft[Seq[String]](Nil) { case (errors, (name, config)) =>
      val pv = config.init.domainParameters.protocolVersion

      if (pv.isEmpty)
        s"Protocol version is not defined for domain `$name`. Define protocol version at key `init.domain-parameters.protocol-version`" +: errors
      else errors
    }
    NonEmpty.from(errors).map(Validated.invalid).getOrElse(Valid)
  }

  private[config] val backwardsCompatibleLoggingConfigErr =
    "Inconsistent configuration of canton.monitoring.log-message-payloads and canton.monitoring.logging.api.message-payloads. Please use the latter in your configuration"

  private def developmentProtocolSafetyCheckDomains(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] =
    developmentProtocolSafetyCheck(
      config.parameters.nonStandardConfig,
      config.domains.toSeq.map { case (k, v) =>
        (k, v.init.domainParameters)
      },
    )

  private def developmentProtocolSafetyCheckParticipants(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    def toNe(
        name: String,
        nonStandardConfig: Boolean,
        devVersionSupport: Boolean,
    ): Validated[NonEmpty[Seq[String]], Unit] =
      Validated.cond(
        nonStandardConfig || !devVersionSupport,
        (),
        NonEmpty(
          Seq,
          s"Enabling dev-version-support for participant $name requires you to explicitly set canton.parameters.non-standard-config = yes",
        ),
      )

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
    val errors = config.participants.toSeq.foldLeft[Seq[String]](Nil) {
      case (errors, (name, config)) =>
        val minimum = config.parameters.minimumProtocolVersion.map(_.unwrap)
        val isMinimumDeprecatedVersion = minimum.getOrElse(ProtocolVersion.minimum).isDeprecated

        if (isMinimumDeprecatedVersion && !config.parameters.dontWarnOnDeprecatedPV)
          DeprecatedProtocolVersion.WarnParticipant(name, minimum).cause +: errors
        else errors
    }

    NonEmpty.from(errors).map(Validated.invalid).getOrElse(Valid)
  }

  private def warnIfDeprecatedProtocolVersionEmbeddedDomain(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val errors = config.domains.toSeq.foldLeft[Seq[String]](Nil) { case (errors, (name, config)) =>
      // If the protocol version is empty, another validation will fail, so we can use latest
      val pv =
        config.init.domainParameters.protocolVersion.fold(ProtocolVersion.latestStable)(_.unwrap)

      if (pv.isDeprecated && !config.init.domainParameters.dontWarnOnDeprecatedPV)
        DeprecatedProtocolVersion.WarnDomain(name, pv).cause +: errors
      else errors
    }
    NonEmpty.from(errors).map(Validated.invalid).getOrElse(Valid)
  }

  private def betaProtocolSafetyCheck(
      config: CantonConfig
  )(parameters: CantonParameters): Validated[NonEmpty[Seq[String]], Unit] = {
    val errors = config.domains.toSeq.foldLeft[Seq[String]](Nil) { case (errors, (name, config)) =>
      // If the protocol version is empty, another validation will fail, so we can use latest
      val pv =
        config.init.domainParameters.protocolVersion.fold(ProtocolVersion.latestStable)(_.unwrap)
      val betaVersionOrDevVersionEnabled =
        config.init.domainParameters.betaVersionSupport || parameters.betaVersionSupport || config.init.domainParameters.devVersionSupport || parameters.devVersionSupport

      if (pv.isBeta && !betaVersionOrDevVersionEnabled)
        s"Using beta protocol $pv for node $name requires you to explicitly set canton.parameters.beta-version-support = yes" +: errors
      else errors
    }
    NonEmpty.from(errors).map(Validated.invalid).getOrElse(Valid)
  }

  private[config] def developmentProtocolSafetyCheck(
      allowUnstableProtocolVersion: Boolean,
      namesAndConfig: Seq[(InstanceName, DomainParametersConfig)],
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    def toNe(
        name: String,
        protocolVersion: ProtocolVersion,
        allowUnstableProtocolVersion: Boolean,
    ): Validated[NonEmpty[Seq[String]], Unit] =
      Validated.cond(
        !protocolVersion.isUnstable || allowUnstableProtocolVersion,
        (),
        NonEmpty(
          Seq,
          s"Using non-stable protocol $protocolVersion for node $name requires you to explicitly set canton.parameters.non-standard-config = yes",
        ),
      )

    namesAndConfig.toList.traverse_ { case (name, parameters) =>
      toNe(
        name.unwrap,
        // If the protocol version is empty, another validation will fail, so we can use latest
        parameters.protocolVersion.fold(ProtocolVersion.latestStable)(_.unwrap),
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
    ): Validated[NonEmpty[Seq[String]], Unit] =
      Validated.cond(
        nonStandardConfig || adminToken.isEmpty,
        (),
        NonEmpty(
          Seq,
          s"Setting ledger-api.admin-token for participant $name requires you to explicitly set canton.parameters.non-standard-config = yes",
        ),
      )

    config.participants.toList.traverse_ { case (name, participantConfig) =>
      toNe(
        name.unwrap,
        config.parameters.nonStandardConfig,
        participantConfig.ledgerApi.adminToken,
      )
    }
  }

  private def adminTokensMatchOnParticipants(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val errors = config.participants.toSeq.mapFilter { case (name, participantConfig) =>
      Option.when(
        participantConfig.ledgerApi.adminToken.exists(la =>
          participantConfig.adminApi.adminToken.exists(_ != la)
        )
      )(
        s"if both ledger-api.admin-token and admin-api.admin-token provided, they must match for participant ${name.unwrap}"
      )
    }
    NonEmpty
      .from(errors)
      .map(Validated.invalid[NonEmpty[Seq[String]], Unit])
      .getOrElse(Validated.Valid(()))
  }

  private def eitherUserListsOrPrivilegedTokensOnParticipants(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val errors = config.participants.toSeq
      .flatMap { case (name, participantConfig) =>
        participantConfig.adminApi.authServices.map(name -> _) ++
          participantConfig.ledgerApi.authServices.map(name -> _)
      }
      .mapFilter { case (name, authService) =>
        Option.when(
          authService.privileged && authService.users.nonEmpty
        )(
          s"authorization service cannot be configured to accept both privileged tokens and tokens for a user-lists in ${name.unwrap}"
        )
      }
    NonEmpty
      .from(errors)
      .map(Validated.invalid[NonEmpty[Seq[String]], Unit])
      .getOrElse(Validated.Valid(()))
  }
}
