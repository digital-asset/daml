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
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.crypto.CryptoSchemes
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HandshakeErrors.DeprecatedProtocolVersion
import com.digitalasset.canton.version.ProtocolVersion

import java.net.URI

private[config] trait ConfigValidations {
  final def validate[T >: CantonConfig](
      config: CantonConfig,
      edition: CantonEdition,
      ensurePortsSet: Boolean,
  )(implicit
      validator: CantonConfigValidator[T]
  ): Validated[NonEmpty[Seq[String]], Unit] =
    config
      .validate[T](edition)
      .toValidated
      .leftMap(_.map(_.toString))
      .combine(validations(ensurePortsSet = ensurePortsSet).traverse_(_(config)))

  type Validation = CantonConfig => Validated[NonEmpty[Seq[String]], Unit]

  /** Return the list of validations
    * @param ensurePortsSet
    *   If set to true, will validate that ports are set. Should be true in `main`.
    * @return
    */
  protected def validations(ensurePortsSet: Boolean): List[Validation]

  protected def toValidated(errors: Seq[String]): Validated[NonEmpty[Seq[String]], Unit] = NonEmpty
    .from(errors)
    .map(Validated.invalid[NonEmpty[Seq[String]], Unit])
    .getOrElse(Validated.Valid(()))
}

object CommunityConfigValidations extends ConfigValidations with NamedLogging {
  import TraceContext.Implicits.Empty.*

  override protected def loggerFactory: NamedLoggerFactory = NamedLoggerFactory.root

  private val Valid: Validated[NonEmpty[Seq[String]], Unit] = Validated.valid(())

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

  override protected def validations(ensurePortsSet: Boolean): List[Validation] =
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
      dbLockFeaturesRequireUsingLockSupportingStorage,
      highlyAvailableSequencerTotalNodeCount,
      noDuplicateStorageUnlessReplicated,
      atLeastOneNode,
      sequencerClientRetryDelays,
      awsKmsDisableSSLVerificationRequiresNonStandard,
    ) ++ (if (ensurePortsSet) List(portsArtSet) else Nil)

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

  private def portsArtSet(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    def toList(predicate: Boolean, value: => String): List[String] =
      if (predicate) List(value) else Nil

    val errors = config.allLocalNodes.toSeq.flatMap { case (name, nodeConfig) =>
      val adminPortNotSetError = toList(
        nodeConfig.adminApi.internalPort.isEmpty,
        portNotSetError(
          nodeType = nodeConfig.nodeTypeName,
          nodeName = name.unwrap,
          service = "admin-api",
        ),
      )

      val nodeSpecificErrors = nodeConfig match {
        case participant: ParticipantNodeConfig =>
          toList(
            participant.httpLedgerApi.enabled && participant.httpLedgerApi.internalPort.isEmpty,
            portNotSetError(
              nodeType = nodeConfig.nodeTypeName,
              nodeName = name.unwrap,
              service = "http-ledger-api",
            ),
          ) ++ toList(
            participant.ledgerApi.internalPort.isEmpty,
            portNotSetError(
              nodeType = nodeConfig.nodeTypeName,
              nodeName = name.unwrap,
              service = "ledger-api",
            ),
          )

        case sequencer: SequencerNodeConfig =>
          toList(
            sequencer.publicApi.internalPort.isEmpty,
            portNotSetError(
              nodeType = nodeConfig.nodeTypeName,
              nodeName = name.unwrap,
              service = "public-api",
            ),
          )

        case _ => Nil
      }

      adminPortNotSetError ++ nodeSpecificErrors
    }

    toValidated(errors)
  }

  def portNotSetError(
      nodeType: String,
      nodeName: String,
      service: String,
      intermediate: String = "",
  ): String =
    s"canton.${nodeType}s.$nodeName.$service.${intermediate}port not set"

  private def alphaProtocolVersionRequiresNonStandard(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {

    val errors = config.allLocalNodes.toSeq.mapFilter { case (name, nodeConfig) =>
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

    val errors = config.allLocalNodes.toSeq.mapFilter {
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
        s"Modifying ledger-api.admin-token-config.* " +
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
    val errors: Seq[String] = config.allLocalNodes.toSeq.flatMap { case (nodeName, nodeConfig) =>
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
    val errors = config.allLocalNodes.toSeq.mapFilter { case (_, nodeConfig) =>
      val cryptoConfig = nodeConfig.crypto
      cryptoConfig.kms match {
        case Some(kmsConfig) =>
          val sessionSigningKeysConfig = kmsConfig.sessionSigningKeys
          cryptoConfig.provider match {
            case CryptoProvider.Kms =>
              val schemesE = CryptoSchemes.fromConfig(cryptoConfig)
              val supportedAlgoSpecs =
                schemesE.map(_.signingSchemes.algorithmSpecs.allowed.forgetNE).getOrElse(Set.empty)
              val supportedKeySpecs =
                schemesE.map(_.signingSchemes.keySpecs.allowed.forgetNE).getOrElse(Set.empty)

              // the signing algorithm spec configured for session keys is not supported
              if (!supportedAlgoSpecs.contains(sessionSigningKeysConfig.signingAlgorithmSpec))
                Some(
                  s"The selected signing algorithm specification, ${sessionSigningKeysConfig.signingAlgorithmSpec}, " +
                    s"for session signing keys is not supported. Supported algorithms " +
                    s"are: $supportedAlgoSpecs."
                )
              // the signing key spec configured for session keys is not supported
              else if (!supportedKeySpecs.contains(sessionSigningKeysConfig.signingKeySpec))
                Some(
                  s"The selected signing key specification, ${sessionSigningKeysConfig.signingKeySpec}, " +
                    s"for session signing keys is not supported. Supported keys " +
                    s"are: $supportedKeySpecs."
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

  private def isUsingHaSequencer(config: SequencerConfig): Boolean =
    PartialFunction.cond(config) {
      // needs to be using both a database sequencer config with that set to use HA
      case dbConfig: SequencerConfig.Database =>
        dbConfig.highAvailabilityEnabled
    }

  private def sequencerClientRetryDelays(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val CantonConfig(
      sequencers,
      mediators,
      participants,
      _,
      _,
      _,
      _,
      _,
      _,
    ) = config

    val sequencerClientConfigs =
      config.allLocalNodes.fmap(_.sequencerClient)

    def checkSequencerClientConfig(
        ref: InstanceName,
        config: SequencerClientConfig,
    ): Validated[NonEmpty[Seq[String]], Unit] =
      Validated.cond(
        config.initialConnectionRetryDelay.underlying <= config.warnDisconnectDelay.underlying &&
          config.warnDisconnectDelay.underlying <= config.maxConnectionRetryDelay.underlying,
        (),
        NonEmpty(
          Seq,
          s"Retry delay configuration for '$ref' sequencer client doesn't respect the condition initialConnectionRetryDelay <= warnDisconnectDelay <= maxConnectionRetryDelay; " +
            s"respective values are (${config.initialConnectionRetryDelay.underlying}, ${config.warnDisconnectDelay.underlying}, ${config.maxConnectionRetryDelay.underlying})",
        ),
      )

    sequencerClientConfigs.toSeq.traverse_ { case (ref, sequencerClientConfig) =>
      checkSequencerClientConfig(ref, sequencerClientConfig)
    }
  }

  private def highlyAvailableSequencerTotalNodeCount(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    def validate(
        nodeType: String,
        name: String,
        sequencerConfig: SequencerConfig,
    ): Validated[NonEmpty[Seq[String]], Unit] =
      Option(sequencerConfig)
        .collect { case dbConfig: SequencerConfig.Database => dbConfig }
        .filter(_.highAvailabilityEnabled)
        .map(
          _.highAvailability
            .getOrElse(throw new IllegalStateException("HA not set despite being enabled"))
            .totalNodeCount
            .unwrap
        )
        .fold(Valid) { totalNodeCount =>
          Validated.cond(
            totalNodeCount < DbLockConfig.MAX_SEQUENCER_WRITERS_AVAILABLE,
            (),
            NonEmpty(
              Seq,
              s"$nodeType node $name sets sequencer HA total node count to $totalNodeCount. Must be less than ${DbLockConfig.MAX_SEQUENCER_WRITERS_AVAILABLE}.",
            ),
          )
        }

    config.sequencersByString
      .map { case (name, config) =>
        validate("Sequencer", name, config.sequencer)
      }
      .toList
      .sequence_
  }

  /** If a participant is using replicated storage or the database sequencer is configured to use
    * its high availability support then it requires using a database that supports locking.
    */
  private def dbLockFeaturesRequireUsingLockSupportingStorage(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val sequencerHaFeature = "the highly available database sequencer"

    def verifySupportedStorage(
        nodeType: String,
        nodeName: String,
        feature: String,
        config: LocalNodeConfig,
    ): Option[String] =
      Option.when(!DbLockConfig.isSupportedConfig(config.storage))(
        s"$nodeType node $nodeName must be configured to use Postgres for storage to use $feature"
      )

    val sequencerNodeValidationErrors = config.sequencersByString
      .filter { case (_, sequencerNodeConfig) =>
        isUsingHaSequencer(sequencerNodeConfig.sequencer)
      }
      .toSeq
      .mapFilter { case (name, node) =>
        verifySupportedStorage("Sequencer", name, sequencerHaFeature, node)
      }

    val participantNodeValidations = config.participantsByString
      .filter(
        _._2.replication.exists(_.isEnabled)
      ) // only replicated participants need to use lock based storage
      .toSeq
      .mapFilter { case (name, node) =>
        verifySupportedStorage("Participant", name, "replication", node)
      }

    toValidated(sequencerNodeValidationErrors ++ participantNodeValidations)
  }

  /** Validate the config that the storage configuration is not shared between nodes unless for
    * replicated participants.
    */
  private def noDuplicateStorageUnlessReplicated(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    def allReplicatedParticipants(nodes: List[(String, LocalNodeConfig)]): Boolean =
      nodes
        .map(_._2)
        .forall(PartialFunction.cond(_) { case cfg: ParticipantNodeConfig =>
          cfg.replication.exists(_.isEnabled)
        })

    def allHighlyAvailableSequencers(nodes: List[(String, LocalNodeConfig)]): Boolean =
      nodes
        .map(_._2)
        .forall(PartialFunction.cond(_) { case cfg: SequencerNodeConfig =>
          isUsingHaSequencer(cfg.sequencer)
        })

    def allHighlyAvailableMediators(nodes: List[(String, LocalNodeConfig)]): Boolean =
      nodes
        .map(_._2)
        .forall(PartialFunction.cond(_) { case cfg: MediatorNodeConfig =>
          cfg.replicationEnabled
        })

    def requiresSharedStorage(nodes: List[(String, LocalNodeConfig)]): Boolean =
      allReplicatedParticipants(nodes) || allHighlyAvailableSequencers(
        nodes
      ) || allHighlyAvailableMediators(nodes)

    val dbAccessToNodes = CommunityConfigValidations.extractNormalizedDbAccess(
      config.participantsByString,
      config.sequencersByString,
      config.mediatorsByString,
    )

    val errors = dbAccessToNodes.toSeq
      .mapFilter {
        case (dbAccess, nodes) if nodes.sizeIs > 1 && !requiresSharedStorage(nodes) =>
          Option(s"Nodes ${formatNodeList(nodes)} share same DB access: $dbAccess")
        case _ => None
      }

    toValidated(errors)
  }

  private def awsKmsDisableSSLVerificationRequiresNonStandard(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {

    val errors = config.allLocalNodes.toSeq.mapFilter { case (name, nodeConfig) =>
      val nonStandardConfig = config.parameters.nonStandardConfig

      nodeConfig.crypto.kms.collect { case aws: KmsConfig.Aws =>
        Option.when(!nonStandardConfig && aws.disableSslVerification)(
          awsKmsDisableSSLVerificationRequiresNonStandardError(
            nodeType = nodeConfig.nodeTypeName,
            nodeName = name.unwrap,
          )
        )
      }.flatten
    }

    toValidated(errors)
  }

  def awsKmsDisableSSLVerificationRequiresNonStandardError(nodeType: String, nodeName: String) =
    s"Disabling SSL verification for AWS KMS on $nodeType $nodeName requires you to explicitly set canton.parameters.non-standard-config = yes"

}
