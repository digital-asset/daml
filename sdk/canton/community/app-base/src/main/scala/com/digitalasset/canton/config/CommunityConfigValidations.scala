// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.data.Validated
import cats.instances.list.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.crypto.CryptoSchemes
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HandshakeErrors.DeprecatedProtocolVersion
import com.digitalasset.canton.version.ProtocolVersion

import java.net.URI

private[config] trait ConfigValidations {
  final def validate[T >: CantonConfig](config: CantonConfig, edition: CantonEdition)(implicit
      validator: CantonConfigValidator[T]
  ): Validated[NonEmpty[Seq[String]], Unit] =
    config
      .validate[T](edition)
      .toValidated
      .leftMap(_.map(_.toString))
      .combine(validations.traverse_(_(config)))

  type Validation = CantonConfig => Validated[NonEmpty[Seq[String]], Unit]

  protected val validations: List[Validation]

  protected def toValidated(errors: Seq[String]): Validated[NonEmpty[Seq[String]], Unit] = NonEmpty
    .from(errors)
    .map(Validated.invalid[NonEmpty[Seq[String]], Unit])
    .getOrElse(Validated.Valid(()))
}

object CommunityConfigValidations extends ConfigValidations with NamedLogging {
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

  override protected val validations: List[Validation] =
    List[Validation](noDuplicateStorage, atLeastOneNode) ++
      genericValidations[CantonConfig]

  /** Validations applied to all community and enterprise Canton configurations. */
  private[config] def genericValidations[C <: CantonConfig]
      : List[C => Validated[NonEmpty[Seq[String]], Unit]] =
    List(
      alphaProtocolVersionRequiresNonStandard,
      dbSequencerRequiresNonStandard,
      snapshotDirRequiresNonStandard,
      warnIfUnsafeMinProtocolVersion,
      adminTokenSafetyCheckParticipants,
      adminTokenConfigsMatchOnParticipants,
      eitherUserListsOrPrivilegedTokensOnParticipants,
      validateSelectedSchemes,
      sessionSigningKeysOnlyWithKms,
      distinctScopesAndAudiencesOnAuthServices,
      engineAdditionalConsistencyChecksParticipants,
    )

  /** Group node configs by db access to find matching db storage configs. Overcomplicated types
    * used are to work around that at this point nodes could have conflicting names so we can't just
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
      config: CantonConfig
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
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val CantonConfig(
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

  private def alphaProtocolVersionRequiresNonStandard(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {

    val errors = config.allNodes.toSeq.mapFilter { case (name, nodeConfig) =>
      val nonStandardConfig = config.parameters.nonStandardConfig
      val alphaVersionSupport = nodeConfig.parameters.alphaVersionSupport
      Option.when(!nonStandardConfig && alphaVersionSupport)(
        alphaProtocolVersionRequiresNonStandardError(
          nodeType = nodeConfig.nodeTypeName,
          nodeName = name.unwrap,
        )
      )
    }

    toValidated(errors)
  }

  def alphaProtocolVersionRequiresNonStandardError(nodeType: String, nodeName: String) =
    s"Enabling alpha-version-support for $nodeType $nodeName requires you to explicitly set canton.parameters.non-standard-config = yes"

  def snapshotDirRequiresNonStandard(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {

    val errors = config.allNodes.toSeq.mapFilter {
      case (name, nodeConfig: ParticipantNodeConfig) =>
        val nonStandardConfig = config.parameters.nonStandardConfig
        val snapshotSupportEnabled = nodeConfig.features.snapshotDir.nonEmpty
        Option.when(!nonStandardConfig && snapshotSupportEnabled)(
          s"Setting snapshot-dir for ${nodeConfig.nodeTypeName} ${name.unwrap} requires you to explicitly set canton.parameters.non-standard-config = yes"
        )

      case _ =>
        None
    }

    toValidated(errors)
  }

  private def dbSequencerRequiresNonStandard(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val errors = if (!config.parameters.nonStandardConfig) {
      config.sequencers.toSeq.mapFilter { case (name, config) =>
        config.sequencer match {
          case _: SequencerConfig.Database => Some(dbSequencerRequiresNonStandardError(name.unwrap))
          case _ => None
        }
      }
    } else Nil

    toValidated(errors)
  }

  def dbSequencerRequiresNonStandardError(nodeName: String): String =
    s"Using DB sequencer config for sequencer $nodeName requires you to explicitly set canton.parameters.non-standard-config = yes"

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
        !config.parameters.nonStandardConfig && (participantConfig.ledgerApi.adminTokenConfig != AdminTokenConfig())
      )(
        s"Setting ledger-api.admin-token-config.fixed-admin-token or ledger-api.admin-token-config.admin-token-duration " +
          s"for participant ${name.unwrap} requires you to explicitly set canton.parameters.non-standard-config = yes"
      )
    }
    toValidated(errors)
  }

  private def adminTokenConfigsMatchOnParticipants(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val errors = config.participants.toSeq.mapFilter { case (name, participantConfig) =>
      Option.when(
        participantConfig.ledgerApi.adminTokenConfig.fixedAdminToken.exists(la =>
          participantConfig.adminApi.adminTokenConfig.fixedAdminToken.exists(_ != la)
        )
      )(
        s"if both ledger-api.admin-token-config.fixed-admin-token and admin-api.admin-token-config.fixed-admin-token provided, they must match for participant ${name.unwrap}"
      )
    }
    toValidated(errors)
  }

  private def engineAdditionalConsistencyChecksParticipants(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val errors = config.participants.toSeq.mapFilter { case (name, participantConfig) =>
      Option.when(
        participantConfig.parameters.engine.enableAdditionalConsistencyChecks && !config.parameters.nonStandardConfig
      )(
        s"Enabling additional consistency checks on the Daml Engine for participant ${name.unwrap} requires to explicitly set canton.parameters.non-standard-config = true"
      )
    }
    toValidated(errors)
  }

  private def validateSelectedSchemes(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val errors: Seq[String] = config.allNodes.toSeq.flatMap { case (nodeName, nodeConfig) =>
      val cryptoConfig = nodeConfig.crypto

      val supportedSigningAlgoSpecs = cryptoConfig.signing.algorithms.allowed
      val supportedSigningKeySpecs = cryptoConfig.signing.keys.allowed
      val supportedEncryptionAlgoSpecs = cryptoConfig.encryption.algorithms.allowed
      val supportedEncryptionKeySpecs = cryptoConfig.encryption.keys.allowed

      def prefixErrors(validated: Seq[String]): Seq[String] =
        validated.map(err => s"Node $nodeName: $err")

      val signingAlgoCheck: Seq[String] =
        cryptoConfig.signing.algorithms.default.zip(supportedSigningAlgoSpecs) match {
          case Some((default, allowed)) if !allowed.contains(default) =>
            Seq(
              s"The selected signing algorithm specification, $default, is not supported. Supported " +
                s"algorithms: $allowed."
            )
          case _ => Nil
        }

      val signingKeyCheck: Seq[String] =
        cryptoConfig.signing.keys.default.zip(supportedSigningKeySpecs) match {
          case Some((default, allowed)) if !allowed.contains(default) =>
            Seq(
              s"The selected signing key specification, $default, is not supported. Supported " +
                s"keys: $allowed."
            )
          case _ => Nil
        }

      val signingKeySpecRelationCheck: Seq[String] =
        supportedSigningKeySpecs match {
          case Some(specs) =>
            val allowedSpecs =
              supportedSigningAlgoSpecs.toList.flatten.flatMap(_.supportedSigningKeySpecs).toSet
            if (allowedSpecs.nonEmpty && !specs.subsetOf(allowedSpecs))
              Seq(
                s"The allowed signing key specifications ($specs) are not all supported by the allowed " +
                  s"signing algorithms. Supported keys for those algorithms are: $allowedSpecs."
              )
            else Nil
          case None => Nil
        }

      val signingAlgoSpecRelationCheck: Seq[String] =
        supportedSigningKeySpecs match {
          case Some(specs) =>
            supportedSigningAlgoSpecs.toList.flatten.foldLeft(List.empty[String]) {
              case (state, algoSpec) =>
                if (algoSpec.supportedSigningKeySpecs.intersect(specs).isEmpty)
                  state :+ s"The signing algorithm specification $algoSpec does not include any key " +
                    s"specification supported by this node. Supported key specifications: $specs."
                else state
            }
          case None => Nil
        }

      val encryptionAlgoCheck: Seq[String] =
        cryptoConfig.encryption.algorithms.default.zip(supportedEncryptionAlgoSpecs) match {
          case Some((default, allowed)) if !allowed.contains(default) =>
            Seq(
              s"The selected encryption algorithm specification, $default, is not supported. Supported " +
                s"algorithms: $allowed."
            )
          case _ => Nil
        }

      val encryptionKeyCheck: Seq[String] =
        cryptoConfig.encryption.keys.default.zip(supportedEncryptionKeySpecs) match {
          case Some((default, allowed)) if !allowed.contains(default) =>
            Seq(
              s"The selected encryption key specification, $default, is not supported. Supported " +
                s"keys: $allowed."
            )
          case _ => Nil
        }

      val encryptionKeySpecRelationCheck: Seq[String] =
        supportedEncryptionKeySpecs match {
          case Some(specs) =>
            val allowedSpecs =
              supportedEncryptionAlgoSpecs.toList.flatten
                .flatMap(_.supportedEncryptionKeySpecs)
                .toSet
            if (allowedSpecs.nonEmpty && !specs.subsetOf(allowedSpecs))
              Seq(
                s"The allowed encryption key specifications ($specs) are not all supported by the " +
                  s"allowed encryption algorithms. Supported keys for those algorithms are: $allowedSpecs."
              )
            else Nil
          case None => Nil
        }

      val encryptionAlgoSpecRelationCheck: Seq[String] =
        supportedEncryptionKeySpecs match {
          case Some(specs) =>
            supportedEncryptionAlgoSpecs.toList.flatten.foldLeft(List.empty[String]) {
              case (state, algoSpec) =>
                if (algoSpec.supportedEncryptionKeySpecs.intersect(specs).isEmpty)
                  state :+ s"The encryption algorithm specification $algoSpec does not include any key " +
                    s"specification supported by this node. Supported key specifications: $specs."
                else state
            }
          case None => Nil
        }

      // only one supported scheme for symmetric, hash, and pbkdf; no need to check them.

      val allChecksForNode =
        signingAlgoCheck ++ signingKeyCheck ++ signingKeySpecRelationCheck ++ signingAlgoSpecRelationCheck ++
          encryptionAlgoCheck ++ encryptionKeyCheck ++ encryptionKeySpecRelationCheck ++ encryptionAlgoSpecRelationCheck

      prefixErrors(allChecksForNode)
    }
    toValidated(errors)
  }

  private def sessionSigningKeysOnlyWithKms(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val errors = config.allNodes.toSeq.mapFilter { case (_, nodeConfig) =>
      val cryptoConfig = nodeConfig.crypto
      cryptoConfig.kms match {
        case Some(kmsConfig) =>
          val sessionSigningKeysConfig = kmsConfig.sessionSigningKeys
          cryptoConfig.provider match {
            case CryptoProvider.Kms =>
              val schemesE = CryptoSchemes.fromConfig(cryptoConfig)
              val supportedAlgoSpecs =
                schemesE.map(_.signingAlgoSpecs.allowed.forgetNE).getOrElse(Set.empty)
              val supportedKeySpecs =
                schemesE.map(_.signingKeySpecs.allowed.forgetNE).getOrElse(Set.empty)

              // the signing algorithm spec configured for session keys is not supported
              if (!supportedAlgoSpecs.contains(sessionSigningKeysConfig.signingAlgorithmSpec))
                Some(
                  s"The selected signing algorithm specification, ${sessionSigningKeysConfig.signingAlgorithmSpec}, " +
                    s"for session signing keys is not supported. Supported algorithms " +
                    s"are: ${cryptoConfig.signing.algorithms.allowed}."
                )
              // the signing key spec configured for session keys is not supported
              else if (!supportedKeySpecs.contains(sessionSigningKeysConfig.signingKeySpec))
                Some(
                  s"The selected signing key specification, ${sessionSigningKeysConfig.signingKeySpec}, " +
                    s"for session signing keys is not supported. Supported keys " +
                    s"are: ${cryptoConfig.signing.keys.allowed}."
                )
              else None
            case _ => None
          }
        case None => None
      }
    }

    toValidated(errors)
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
          s"Authorization service cannot be configured to accept both privileged tokens and tokens for user-lists in ${name.unwrap}"
        )
      }
    NonEmpty
      .from(errors)
      .map(Validated.invalid[NonEmpty[Seq[String]], Unit])
      .getOrElse(Validated.Valid(()))
  }

  private def distinctScopesAndAudiencesOnAuthServices(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    def checkDuplicates(
        name: String,
        attrName: String,
        extract: AuthServiceConfig => Option[String],
        authServices: Seq[AuthServiceConfig],
    ) = {
      val attributes = authServices.flatMap(extract)
      Option
        .when(
          attributes.sizeIs != attributes.distinct.sizeIs
        )(
          s"Multiple authorization service configured with the same $attrName in $name"
        )
        .toList
    }
    val errors = config.participants.toSeq
      .flatMap { case (name, participantConfig) =>
        List(
          name -> participantConfig.adminApi.authServices,
          name -> participantConfig.ledgerApi.authServices,
        )
      }
      .flatMap { case (name, authServices) =>
        checkDuplicates(name.unwrap, "scope", _.targetScope, authServices) ++
          checkDuplicates(name.unwrap, "audience", _.targetAudience, authServices)
      }
    NonEmpty
      .from(errors)
      .map(Validated.invalid[NonEmpty[Seq[String]], Unit])
      .getOrElse(Validated.Valid(()))
  }
}
