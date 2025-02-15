// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.data.Validated
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.crypto.CryptoFactory
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HandshakeErrors.DeprecatedProtocolVersion
import com.digitalasset.canton.version.ProtocolVersion

import java.net.URI

private[config] trait ConfigValidations[C <: CantonConfig] {
  final def validate[T >: C](config: C)(implicit
      validator: CantonConfigValidator[T]
  ): Validated[NonEmpty[Seq[String]], Unit] =
    config
      .validate[T](edition)
      .toValidated
      .leftMap(_.map(_.toString))
      .combine(validations.traverse_(_(config)))

  protected def edition: CantonEdition

  protected val validations: List[C => Validated[NonEmpty[Seq[String]], Unit]]

  protected def toValidated(errors: Seq[String]): Validated[NonEmpty[Seq[String]], Unit] = NonEmpty
    .from(errors)
    .map(Validated.invalid[NonEmpty[Seq[String]], Unit])
    .getOrElse(Validated.Valid(()))
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

  override protected val edition: CantonEdition = CommunityCantonEdition

  type Validation = CantonCommunityConfig => Validated[NonEmpty[Seq[String]], Unit]

  override protected val validations: List[Validation] =
    List[Validation](noDuplicateStorage, atLeastOneNode) ++
      genericValidations[CantonCommunityConfig]

  /** Validations applied to all community and enterprise Canton configurations. */
  private[config] def genericValidations[C <: CantonConfig]
      : List[C => Validated[NonEmpty[Seq[String]], Unit]] =
    List(
      developmentProtocolSafetyCheck,
      warnIfUnsafeMinProtocolVersion,
      adminTokenSafetyCheckParticipants,
      adminTokensMatchOnParticipants,
      sessionSigningKeysOnlyWithKms,
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
          url <- dbConfig match {
            case _: DbConfig.H2 => Some(DbConfig.h2Url(dbName))
            case _: DbConfig.Postgres => Some(DbConfig.postgresUrl(server, port, dbName))
            case other => throw new IllegalArgumentException(s"Unsupported DbConfig: $other")
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
      extractNormalizedDbAccess(
        config.participantsByString,
        config.sequencersByString,
        config.mediatorsByString,
      )

    val errors = dbAccessToNodes.toSeq
      .mapFilter {
        case (dbAccess, nodes) if nodes.lengthCompare(1) > 0 =>
          Option(s"Nodes ${formatNodeList(nodes)} share same DB access: $dbAccess")
        case _ => None
      }
    toValidated(errors)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private def atLeastOneNode(
      config: CantonCommunityConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val CantonCommunityConfig(
      participants,
      sequencers,
      mediators,
      remoteParticipants,
      remoteSequencers,
      remoteMediators,
      _,
      _,
      _,
    ) =
      config
    Validated.cond(
      Seq(
        participants,
        remoteParticipants,
        mediators,
        remoteMediators,
        sequencers,
        remoteSequencers,
      )
        .exists(_.nonEmpty),
      (),
      NonEmpty(Seq, "At least one node must be defined in the configuration"),
    )

  }

  private def developmentProtocolSafetyCheck(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {

    val errors = config.allNodes.toSeq.mapFilter { case (name, nodeConfig) =>
      val nonStandardConfig = config.parameters.nonStandardConfig
      val alphaVersionSupport = nodeConfig.parameters.alphaVersionSupport
      Option.when(!nonStandardConfig && alphaVersionSupport)(
        s"Enabling alpha-version-support for ${nodeConfig.nodeTypeName} ${name.unwrap} requires you to explicitly set canton.parameters.non-standard-config = yes"
      )
    }

    toValidated(errors)
  }

  private def warnIfUnsafeMinProtocolVersion(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val errors = config.participants.toSeq.mapFilter { case (name, config) =>
      val minimum = config.parameters.minimumProtocolVersion.map(_.unwrap)
      val isMinimumDeprecatedVersion = minimum.getOrElse(ProtocolVersion.minimum).isDeprecated

      Option.when(isMinimumDeprecatedVersion && !config.parameters.dontWarnOnDeprecatedPV)(
        DeprecatedProtocolVersion.WarnParticipant(name, minimum).cause
      )
    }

    toValidated(errors)
  }

  private def adminTokenSafetyCheckParticipants(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val errors = config.participants.toSeq.mapFilter { case (name, participantConfig) =>
      Option.when(
        !config.parameters.nonStandardConfig && participantConfig.ledgerApi.adminToken.nonEmpty
      )(
        s"Setting ledger-api.admin-token for participant ${name.unwrap} requires you to explicitly set canton.parameters.non-standard-config = yes"
      )
    }
    toValidated(errors)
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
    toValidated(errors)
  }

  private def sessionSigningKeysOnlyWithKms(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val errors = config.allNodes.toSeq.mapFilter { case (name, nodeConfig) =>
      val cryptoConfig = nodeConfig.crypto
      val sessionSigningKeysConfig = nodeConfig.parameters.sessionSigningKeys

      cryptoConfig.provider match {
        case CryptoProvider.Jce if !sessionSigningKeysConfig.enabled => None
        case CryptoProvider.Jce =>
          Some(
            s"Session signing keys should not be enabled with the JCE crypto provider on node ${name.unwrap}"
          )
        case CryptoProvider.Kms if !sessionSigningKeysConfig.enabled => None
        case CryptoProvider.Kms =>
          // If no allowed specifications are configured, all supported specifications of the current provider
          // are allowed, so we must consider those as well.
          val supportedAlgorithms = CryptoFactory
            .selectAllowedSigningAlgorithmSpecs(cryptoConfig)
            .map(_.forgetNE)
            .getOrElse(Set.empty)
          val supportedKeys = CryptoFactory
            .selectAllowedSigningKeySpecs(cryptoConfig)
            .map(_.forgetNE)
            .getOrElse(Set.empty)

          // the signing algorithm spec configured for session keys is not supported
          if (!supportedAlgorithms.contains(sessionSigningKeysConfig.signingAlgorithmSpec))
            Some(
              s"The selected signing algorithm specification, ${sessionSigningKeysConfig.signingAlgorithmSpec}, " +
                s"for session signing keys is not supported. Supported algorithms " +
                s"are: ${cryptoConfig.signing.algorithms.allowed}."
            )
          // the signing key spec configured for session keys is not supported
          else if (!supportedKeys.contains(sessionSigningKeysConfig.signingKeySpec))
            Some(
              s"The selected signing key specification, ${sessionSigningKeysConfig.signingKeySpec}, " +
                s"for session signing keys is not supported. Supported algorithms " +
                s"are: ${cryptoConfig.signing.keys.allowed}."
            )
          else None
      }
    }

    toValidated(errors)
  }
}
