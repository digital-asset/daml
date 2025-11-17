// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.data.Validated.Valid
import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.auth.AuthorizedUser
import com.digitalasset.canton.config.CantonRequireTypes.{InstanceName, NonEmptyString}
import com.digitalasset.canton.config.CommunityConfigValidations.*
import com.digitalasset.canton.config.CryptoProvider.Kms
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.crypto.{
  EncryptionAlgorithmSpec,
  EncryptionKeySpec,
  SigningAlgorithmSpec,
  SigningKeySpec,
}
import com.digitalasset.canton.http.JsonApiConfig
import com.digitalasset.canton.integration.ConfigTransforms
import com.digitalasset.canton.participant.config.{
  LedgerApiServerConfig,
  ParticipantFeaturesConfig,
  ParticipantNodeConfig,
  ParticipantNodeParameterConfig,
}
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.synchronizer.config.PublicServerConfig
import com.digitalasset.canton.synchronizer.mediator.{
  MediatorNodeConfig,
  MediatorNodeParameterConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.SequencerHighAvailabilityConfig
import com.digitalasset.canton.synchronizer.sequencer.config.{
  SequencerNodeConfig,
  SequencerNodeParameterConfig,
}
import com.typesafe.config.ConfigFactory
import monocle.macros.syntax.lens.*
import org.scalatest.prop.{TableFor1, TableFor2}

import java.nio.file.Files
import scala.language.implicitConversions

class CommunityConfigValidationsTest extends BaseTestWordSpec {
  def assertErrors(config: CantonConfig, ensurePortsSet: Boolean = false)(
      expectedMessages: String*
  ): Unit = {
    val validationResult =
      CommunityConfigValidations.validate(
        config,
        EnterpriseCantonEdition,
        ensurePortsSet = ensurePortsSet,
      )

    withClue("expecting configuration to be invalid") {
      validationResult.isValid shouldBe false
    }

    validationResult.toEither.left.value.forgetNE should contain theSameElementsAs expectedMessages
  }

  def assertValid(config: CantonConfig, ensurePortsSet: Boolean = false): Unit = {
    val validationResult =
      CommunityConfigValidations.validate(
        config,
        EnterpriseCantonEdition,
        ensurePortsSet = ensurePortsSet,
      )

    withClue("expecting configuration to be valid") {
      validationResult shouldBe Valid(())
    }
  }

  "ha sequencer total node count validation" should {
    "complain if we're above the maximum configured" in {
      val config = mkHASequencerEnvironmentConfig(
        SequencerHighAvailabilityConfig(
          enabled = Some(true),
          // a valid total node counter should be less than this constant
          totalNodeCount = PositiveInt.tryCreate(DbLockConfig.MAX_SEQUENCER_WRITERS_AVAILABLE),
        )
      )

      assertErrors(config)(
        "Sequencer node s1 sets sequencer HA total node count to 32. Must be less than 32."
      )
    }
  }

  "replication should default to true only if unset and for supported storage" in {
    val h2Config = DbConfig.H2(ConfigFactory.empty())
    val postgresConfig = DbConfig.Postgres(ConfigFactory.empty())

    val instanceName = InstanceName.tryCreate("p1")

    def makeConfig(enabledReplication: Option[Boolean], storageConfig: StorageConfig) =
      CantonConfig(
        participants = Map(
          instanceName -> ParticipantNodeConfig(
            // h2 doesn't support locks
            storage = storageConfig,
            // We're leaving the enabled field unset
            replication =
              enabledReplication.map(enabled => ReplicationConfig(enabled = Some(enabled))),
            ledgerApi = LedgerApiServerConfig(internalPort = Port.tryCreate(3000).some),
            adminApi = AdminServerConfig(internalPort = Port.tryCreate(3001).some),
          )
        ),
        mediators = Map(
          instanceName -> MediatorNodeConfig(
            storage = storageConfig,
            replication = Some(ReplicationConfig(enabled = enabledReplication)),
          )
        ),
        sequencers = Map(
          instanceName -> SequencerNodeConfig(
            storage = storageConfig,
            sequencer = SequencerConfig.Database(
              highAvailability = Some(SequencerHighAvailabilityConfig(enabled = enabledReplication))
            ),
          )
        ),
      ).withDefaults(Some(DefaultPorts.create()), EnterpriseCantonEdition)

    val defaultEnabledMatrix: TableFor2[StorageConfig, Boolean] = Table(
      ("storage", "isEnabled"),
      (h2Config, false),
      (postgresConfig, true),
    )

    def dbSequencerStorageCheck(config: SequencerConfig, enabled: Boolean) =
      config match {
        case db: SequencerConfig.Database =>
          db.highAvailabilityEnabled shouldBe enabled
        case _ =>
      }

    forAll(defaultEnabledMatrix) { case (storage, expected) =>
      makeConfig(None, storage)
        .participants(instanceName)
        .replication
        .exists(_.isEnabled) shouldBe expected
      makeConfig(None, storage).mediators(instanceName).replicationEnabled shouldBe expected
      dbSequencerStorageCheck(
        makeConfig(None, storage).sequencers(instanceName).sequencer,
        expected,
      )
    }

    val explicitEnabledConfig = Table("enabled", true, false)
    val storages: TableFor1[StorageConfig] =
      Table("storage", h2Config, postgresConfig)
    forAll(explicitEnabledConfig) { enabled =>
      forAll(storages) { storage =>
        val config = makeConfig(Some(enabled), storage)
        config
          .participants(instanceName)
          .replication
          .exists(_.isEnabled) shouldBe enabled
        config
          .mediators(instanceName)
          .replicationEnabled shouldBe enabled
        dbSequencerStorageCheck(
          config.sequencers(instanceName).sequencer,
          enabled,
        )
      }
    }
  }

  "replicated participant should use lock supporting storage" in {
    val config = CantonConfig(
      participants = Map(
        InstanceName.tryCreate("p1") -> ParticipantNodeConfig(
          // h2 doesn't support locks
          storage = DbConfig.H2(ConfigFactory.empty()),
          // but we're going to turn on replication which requires locks
          replication = Some(ReplicationConfig(enabled = Some(true))),
          ledgerApi = LedgerApiServerConfig(internalPort = Port.tryCreate(3000).some),
          adminApi = AdminServerConfig(internalPort = Port.tryCreate(3001).some),
        )
      )
    )

    assertErrors(config)(
      "Participant node p1 must be configured to use Postgres for storage to use replication"
    )
  }

  "ha sequencer is using supported storage validation" should {
    val validConfig = mkHASequencerEnvironmentConfig()

    "not complain if configured correctly" in assertValid(validConfig)

    "complain if nodes are not using a supported storage configuration" in {
      // switch to using a memory based storage
      val config = ConfigTransforms.allInMemory(validConfig)

      assertErrors(config)(
        "Sequencer node s1 must be configured to use Postgres for storage to use the highly available database sequencer"
      )
    }

    "not complain if using sequencers without HA and without a locking supporting database" in {
      def disableHASequencer: SequencerConfig = SequencerConfig.Database(
        highAvailability = Some(SequencerHighAvailabilityConfig(enabled = Some(false)))
      )
      val config = ConfigTransforms.allInMemory
        .andThen(
          ConfigTransforms.updateSequencerConfig("s1")(
            _.focus(_.sequencer).replace(disableHASequencer)
          )
        )(validConfig)

      assertValid(config)
    }
  }

  private def mkHASequencerEnvironmentConfig(
      haConfig: SequencerHighAvailabilityConfig =
        SequencerHighAvailabilityConfig(enabled = Some(true))
  ): CantonConfig =
    CantonConfig(
      // DB Sequencer is currently marked as experimental, enabling non-standard config to allow it
      parameters = CantonParameters(nonStandardConfig = true),
      sequencers = Map(
        InstanceName.tryCreate("s1") -> SequencerNodeConfig(
          storage = DbConfig.Postgres(ConfigFactory.empty()),
          sequencer = SequencerConfig.Database(
            highAvailability = Some(haConfig)
          ),
          publicApi = PublicServerConfig(internalPort = Port.tryCreate(2000).some),
          adminApi = AdminServerConfig(internalPort = Port.tryCreate(2001).some),
        )
      ),
    )

  "generic config validations" should {
    "complain if config is empty" in {
      val config = CantonConfig()
      assertErrors(config)("At least one node must be defined in the configuration")
    }

    "complain if port is not set and validation is enabled" in {
      implicit def toPort(i: Int): Port = Port.tryCreate(i)

      def getErrors(config: CantonConfig, ensurePortsSet: Boolean): Seq[String] =
        CommunityConfigValidations
          .validate(
            config,
            EnterpriseCantonEdition,
            ensurePortsSet = ensurePortsSet,
          )
          .toEither
          .fold(_.forgetNE, _ => Nil)

      // config with all ports sets
      val config = CantonConfig(
        parameters = CantonParameters(),
        sequencers = Map(
          InstanceName.tryCreate("s1") -> SequencerNodeConfig(
            publicApi = PublicServerConfig(internalPort = Some(5001)),
            adminApi = AdminServerConfig(internalPort = Some(5002)),
          )
        ),
        mediators = Map(
          InstanceName.tryCreate("m1") -> MediatorNodeConfig(
            adminApi = AdminServerConfig(internalPort = Some(6002))
          )
        ),
        participants = Map(
          InstanceName.tryCreate("p1") -> ParticipantNodeConfig(
            adminApi = AdminServerConfig(internalPort = Some(4002)),
            ledgerApi = LedgerApiServerConfig(internalPort = Some(4001)),
            httpLedgerApi = JsonApiConfig(internalPort = Some(4003)),
          )
        ),
      )

      getErrors(config, ensurePortsSet = false) shouldBe empty
      getErrors(config, ensurePortsSet = true) shouldBe empty

      val unsetPortConfigTransforms = Seq(
        // sequencer
        ConfigTransforms.updateSequencerConfig("s1")(
          _.focus(_.publicApi.internalPort).replace(None)
        ),
        ConfigTransforms.updateSequencerConfig("s1")(
          _.focus(_.adminApi.internalPort).replace(None)
        ),
        // mediator
        ConfigTransforms.updateMediatorConfig("m1")(_.focus(_.adminApi.internalPort).replace(None)),
        // participant
        ConfigTransforms.updateParticipantConfig("p1")(
          _.focus(_.ledgerApi.internalPort).replace(None)
        ),
        ConfigTransforms.updateParticipantConfig("p1")(
          _.focus(_.httpLedgerApi.internalPort).replace(None)
        ),
        ConfigTransforms.updateParticipantConfig("p1")(
          _.focus(_.adminApi.internalPort).replace(None)
        ),
      )

      forAll(unsetPortConfigTransforms) { transform =>
        val updatedConfig = transform(config)

        getErrors(updatedConfig, ensurePortsSet = false) shouldBe empty
        getErrors(updatedConfig, ensurePortsSet = true).loneElement should (include(
          ".port not set"
        ))
      }
    }

    "prevent DB sequencer in sequencer configuration without non-standard config option" in {
      val config = CantonConfig(
        parameters = CantonParameters(),
        sequencers = Map(
          InstanceName.tryCreate("s1") -> SequencerNodeConfig(
            sequencer = SequencerConfig.Database()
          )
        ),
      )
      assertErrors(config)(dbSequencerRequiresNonStandardError("s1"))
      assertValid(config.copy(parameters = CantonParameters(nonStandardConfig = true)))
    }

    "prevent dev protocol in sequencer configurations without non-standard-config option" in {
      val config = CantonConfig(
        parameters = CantonParameters(),
        sequencers = Map(
          InstanceName.tryCreate("s1") -> SequencerNodeConfig(
            parameters = SequencerNodeParameterConfig(alphaVersionSupport = true)
          )
        ),
      )
      assertErrors(config)(alphaProtocolVersionRequiresNonStandardError("sequencer", "s1"))
      assertValid(config.copy(parameters = CantonParameters(nonStandardConfig = true)))
    }

    "prevent dev protocol in mediator configurations without non-standard-config option" in {
      val config = CantonConfig(
        parameters = CantonParameters(),
        mediators = Map(
          InstanceName.tryCreate("m1") -> MediatorNodeConfig(
            parameters = MediatorNodeParameterConfig(alphaVersionSupport = true)
          )
        ),
      )
      assertErrors(config)(alphaProtocolVersionRequiresNonStandardError("mediator", "m1"))
      assertValid(config.copy(parameters = CantonParameters(nonStandardConfig = true)))
    }

    "prevent enabling alpha-version-support in participants without non-standard-config option" in {
      val config = CantonConfig(
        parameters = CantonParameters(),
        participants = Map(
          InstanceName.tryCreate("p1") -> ParticipantNodeConfig(
            parameters = ParticipantNodeParameterConfig(alphaVersionSupport = true)
          )
        ),
      )
      assertErrors(config)(alphaProtocolVersionRequiresNonStandardError("participant", "p1"))
      assertValid(config.copy(parameters = CantonParameters(nonStandardConfig = true)))
    }

    "prevent disabling SSH validation in AWS KMS for sequencers without non-standard-config option" in {
      val config = CantonConfig(
        parameters = CantonParameters(),
        sequencers = Map(
          InstanceName.tryCreate("s1") -> SequencerNodeConfig(
            crypto = CryptoConfig(kms =
              Some(KmsConfig.Aws(region = "test", disableSslVerification = true))
            )
          )
        ),
      )
      assertErrors(config)(
        awsKmsDisableSSLVerificationRequiresNonStandardError("sequencer", "s1")
      )
      assertValid(config.copy(parameters = CantonParameters(nonStandardConfig = true)))
    }

    "prevent disabling SSH validation in AWS KMS for mediators without non-standard-config option" in {
      val config = CantonConfig(
        parameters = CantonParameters(),
        mediators = Map(
          InstanceName.tryCreate("m1") -> MediatorNodeConfig(
            crypto = CryptoConfig(kms =
              Some(KmsConfig.Aws(region = "test", disableSslVerification = true))
            )
          )
        ),
      )
      assertErrors(config)(
        awsKmsDisableSSLVerificationRequiresNonStandardError("mediator", "m1")
      )
      assertValid(config.copy(parameters = CantonParameters(nonStandardConfig = true)))
    }

    "prevent disabling SSH validation in AWS KMS for participants without non-standard-config option" in {
      val config = CantonConfig(
        parameters = CantonParameters(),
        participants = Map(
          InstanceName.tryCreate("p1") -> ParticipantNodeConfig(
            crypto = CryptoConfig(kms =
              Some(KmsConfig.Aws(region = "test", disableSslVerification = true))
            )
          )
        ),
      )
      assertErrors(config)(
        awsKmsDisableSSLVerificationRequiresNonStandardError("participant", "p1")
      )
      assertValid(config.copy(parameters = CantonParameters(nonStandardConfig = true)))
    }

    "prevent use of profile-dir in participants without non-standard-config option" in {
      val config = CantonConfig(
        parameters = CantonParameters(nonStandardConfig = false),
        participants = Map(
          InstanceName.tryCreate("p1") -> ParticipantNodeConfig(
            features =
              ParticipantFeaturesConfig(profileDir = Some(Files.createTempDirectory("testing")))
          )
        ),
      )
      assertValid(config)
    }

    "allow use of profile-dir in participants with non-standard-config option" in {
      val config = CantonConfig(
        parameters = CantonParameters(nonStandardConfig = true),
        participants = Map(
          InstanceName.tryCreate("p1") -> ParticipantNodeConfig(
            features =
              ParticipantFeaturesConfig(profileDir = Some(Files.createTempDirectory("testing")))
          )
        ),
      )
      assertValid(config)
    }

    "prevent enabling snapshot-dir in participants without non-standard config option" in {
      val config = CantonConfig(
        parameters = CantonParameters(nonStandardConfig = false),
        participants = Map(
          InstanceName.tryCreate("p1") -> ParticipantNodeConfig(
            features =
              ParticipantFeaturesConfig(snapshotDir = Some(Files.createTempDirectory("testing")))
          )
        ),
      )
      assertErrors(config)(
        "Setting snapshot-dir for participant p1 requires you to explicitly set canton.parameters.non-standard-config = yes"
      )
      assertValid(config.copy(parameters = CantonParameters(nonStandardConfig = true)))
    }

    "allow use of snapshot-dir in participants with non-standard-config option" in {
      val config = CantonConfig(
        parameters = CantonParameters(nonStandardConfig = true),
        participants = Map(
          InstanceName.tryCreate("p1") -> ParticipantNodeConfig(
            features =
              ParticipantFeaturesConfig(snapshotDir = Some(Files.createTempDirectory("testing")))
          )
        ),
      )
      assertValid(config)
    }
  }

  "sequencer client retry delay checks" when {
    "invalid retry delay settings provided" should {
      val config = CantonConfig(
        sequencers = Map(
          InstanceName.tryCreate("s1") -> SequencerNodeConfig(
            sequencerClient = SequencerClientConfig(
              initialConnectionRetryDelay = NonNegativeFiniteDuration.ofSeconds(5),
              warnDisconnectDelay = NonNegativeFiniteDuration.ofSeconds(1),
              maxConnectionRetryDelay = NonNegativeFiniteDuration.ofMillis(100),
            )
          )
        )
      )
      "fail" in {
        assertErrors(config)(
          "Retry delay configuration for 's1' sequencer client doesn't respect the condition initialConnectionRetryDelay <= warnDisconnectDelay <= maxConnectionRetryDelay; respective values are (5 seconds, 1 second, 100 milliseconds)"
        )
      }
    }

    "valid config provided" should {
      val config = CantonConfig(
        sequencers = Map(
          InstanceName.tryCreate("s1") -> SequencerNodeConfig()
        )
      )
      "succeed" in {
        assertValid(config)
      }
    }
  }

  "nodes with crypto scheme configurations" should {
    def configWithSchemes(
        signingSchemeConfig: Option[SigningSchemeConfig] = None,
        encryptionSchemeConfig: Option[EncryptionSchemeConfig] = None,
    ): CantonConfig =
      CantonConfig(
        participants = Map(
          InstanceName.tryCreate("p1") -> ParticipantNodeConfig(
            crypto = CryptoConfig(
              provider = Kms,
              signing = signingSchemeConfig.getOrElse(SigningSchemeConfig()),
              encryption = encryptionSchemeConfig.getOrElse(EncryptionSchemeConfig()),
            )
          )
        )
      )

    "fail config validation when default crypto scheme is unsupported" in {
      val configInvalid = configWithSchemes(
        signingSchemeConfig = Some(
          SigningSchemeConfig(
            algorithms = CryptoSchemeConfig(
              default = Some(SigningAlgorithmSpec.Ed25519),
              allowed = Some(NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha256)),
            ),
            keys = CryptoSchemeConfig(
              default = Some(SigningKeySpec.EcCurve25519),
              allowed = Some(NonEmpty.mk(Set, SigningKeySpec.EcP256)),
            ),
          )
        ),
        encryptionSchemeConfig = Some(
          EncryptionSchemeConfig(
            algorithms = CryptoSchemeConfig(
              default = Some(EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc),
              allowed = Some(NonEmpty.mk(Set, EncryptionAlgorithmSpec.RsaOaepSha256)),
            ),
            keys = CryptoSchemeConfig(
              default = Some(EncryptionKeySpec.EcP256),
              allowed = Some(NonEmpty.mk(Set, EncryptionKeySpec.Rsa2048)),
            ),
          )
        ),
      )
      assertErrors(configInvalid)(
        s"Node p1: The selected signing algorithm specification, ${SigningAlgorithmSpec.Ed25519}, " +
          s"is not supported. Supported algorithms: ${NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha256)}.",
        s"Node p1: The selected signing key specification, ${SigningKeySpec.EcCurve25519}, " +
          s"is not supported. Supported keys: ${NonEmpty.mk(Set, SigningKeySpec.EcP256)}.",
        s"Node p1: The selected encryption algorithm specification, ${EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc}, " +
          s"is not supported. Supported algorithms: ${NonEmpty.mk(Set, EncryptionAlgorithmSpec.RsaOaepSha256)}.",
        s"Node p1: The selected encryption key specification, ${EncryptionKeySpec.EcP256}, " +
          s"is not supported. Supported keys: ${NonEmpty.mk(Set, EncryptionKeySpec.Rsa2048)}.",
      )
    }

    "fail config validation when key specs aren’t covered by the supported algorithms" in {
      val signingKeySpecsInvalid = configWithSchemes(
        signingSchemeConfig = Some(
          SigningSchemeConfig(
            keys = CryptoSchemeConfig(
              default = Some(SigningKeySpec.EcP256),
              allowed = Some(NonEmpty.mk(Set, SigningKeySpec.EcP256)),
            ),
            algorithms = CryptoSchemeConfig(
              default = Some(SigningAlgorithmSpec.EcDsaSha384),
              allowed = Some(NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha384)),
            ),
          )
        )
      )
      assertErrors(signingKeySpecsInvalid)(
        s"Node p1: The allowed signing key specifications (${NonEmpty.mk(Set, SigningKeySpec.EcP256)}) are not all supported by the allowed signing " +
          s"algorithms. Supported keys for those algorithms are: ${SigningAlgorithmSpec.EcDsaSha384.supportedSigningKeySpecs}.",
        s"Node p1: The signing algorithm specification ${SigningAlgorithmSpec.EcDsaSha384} does not include any key " +
          s"specification supported by this node. Supported key specifications: ${NonEmpty.mk(Set, SigningKeySpec.EcP256)}.",
      )

      val encryptionKeySpecsInvalid = configWithSchemes(
        encryptionSchemeConfig = Some(
          EncryptionSchemeConfig(
            keys = CryptoSchemeConfig(
              default = Some(EncryptionKeySpec.EcP256),
              allowed = Some(NonEmpty.mk(Set, EncryptionKeySpec.EcP256)),
            ),
            algorithms = CryptoSchemeConfig(
              default = Some(EncryptionAlgorithmSpec.RsaOaepSha256),
              allowed = Some(NonEmpty.mk(Set, EncryptionAlgorithmSpec.RsaOaepSha256)),
            ),
          )
        )
      )
      assertErrors(encryptionKeySpecsInvalid)(
        s"Node p1: The allowed encryption key specifications (${NonEmpty.mk(Set, EncryptionKeySpec.EcP256)}) are not all supported by the allowed encryption " +
          s"algorithms. Supported keys for those algorithms are: ${EncryptionAlgorithmSpec.RsaOaepSha256.supportedEncryptionKeySpecs}.",
        s"Node p1: The encryption algorithm specification ${EncryptionAlgorithmSpec.RsaOaepSha256} does not include any key " +
          s"specification supported by this node. Supported key specifications: ${NonEmpty.mk(Set, EncryptionKeySpec.EcP256)}.",
      )
    }

    "fail config validation when an algorithm does not have a supported key spec " in {
      val signingKeySpecsInvalid = configWithSchemes(
        signingSchemeConfig = Some(
          SigningSchemeConfig(
            keys = CryptoSchemeConfig(
              default = Some(SigningKeySpec.EcP256),
              allowed = Some(NonEmpty.mk(Set, SigningKeySpec.EcP256)),
            ),
            algorithms = CryptoSchemeConfig(
              default = Some(SigningAlgorithmSpec.EcDsaSha384),
              allowed = Some(
                NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha256, SigningAlgorithmSpec.EcDsaSha384)
              ),
            ),
          )
        )
      )
      assertErrors(signingKeySpecsInvalid)(
        s"Node p1: The signing algorithm specification ${SigningAlgorithmSpec.EcDsaSha384} does not include any key " +
          s"specification supported by this node. Supported key specifications: ${NonEmpty.mk(Set, SigningKeySpec.EcP256)}."
      )

      val encryptionKeySpecsInvalid = configWithSchemes(
        encryptionSchemeConfig = Some(
          EncryptionSchemeConfig(
            keys = CryptoSchemeConfig(
              default = Some(EncryptionKeySpec.EcP256),
              allowed = Some(NonEmpty.mk(Set, EncryptionKeySpec.EcP256)),
            ),
            algorithms = CryptoSchemeConfig(
              default = Some(EncryptionAlgorithmSpec.RsaOaepSha256),
              allowed = Some(
                NonEmpty.mk(
                  Set,
                  EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc,
                  EncryptionAlgorithmSpec.RsaOaepSha256,
                )
              ),
            ),
          )
        )
      )
      assertErrors(encryptionKeySpecsInvalid)(
        s"Node p1: The encryption algorithm specification ${EncryptionAlgorithmSpec.RsaOaepSha256} does not include any key " +
          s"specification supported by this node. Supported key specifications: ${NonEmpty.mk(Set, EncryptionKeySpec.EcP256)}."
      )

    }

  }

  "nodes using session signing keys" should {
    def sessionSigningKeysConfigWithAllowed(
        allowed: Option[NonEmpty[Set[SigningAlgorithmSpec]]],
        signingAlgorithmSpec: SigningAlgorithmSpec,
    ): CantonConfig =
      CantonConfig(
        participants = Map(
          InstanceName.tryCreate("p1") -> ParticipantNodeConfig(
            crypto = CryptoConfig(
              provider = Kms,
              signing = SigningSchemeConfig(
                algorithms = CryptoSchemeConfig(
                  default = Some(SigningAlgorithmSpec.EcDsaSha256),
                  allowed = allowed,
                )
              ),
              kms = Some(
                KmsConfig.Aws.defaultTestConfig.copy(sessionSigningKeys =
                  SessionSigningKeysConfig.default.copy(signingAlgorithmSpec = signingAlgorithmSpec)
                )
              ),
            )
          )
        )
      )

    "pass config validation with a KMS supported session signing algorithm specification when allowed algorithms are not specified" in {
      CryptoProvider.Kms.signingAlgorithms.supported.foreach { signingAlgorithmSpec =>
        val config = sessionSigningKeysConfigWithAllowed(None, signingAlgorithmSpec)
        assertValid(config)
      }
    }

    "fail config validation with unsupported session signing algorithm specification" in {
      val supportedSigningAlgorithmSchemes: NonEmpty[Set[SigningAlgorithmSpec]] =
        NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha256)
      val signingAlgorithmSpec = SigningAlgorithmSpec.Ed25519

      val config = sessionSigningKeysConfigWithAllowed(
        Some(supportedSigningAlgorithmSchemes),
        signingAlgorithmSpec,
      )

      assertErrors(config)(
        s"The selected signing algorithm specification, $signingAlgorithmSpec, for session signing keys is not " +
          s"supported. Supported algorithms are: $supportedSigningAlgorithmSchemes."
      )
    }

    "fail config validation with unsupported session signing key specification" in {
      val supportedSigningKeySchemes: NonEmpty[Set[SigningKeySpec]] =
        NonEmpty.mk(Set, SigningKeySpec.EcP256)
      val signingKeySpec = SigningKeySpec.EcP256
      val sessionSigningKeySpec = SigningKeySpec.EcCurve25519

      val config = CantonConfig(
        participants = Map(
          InstanceName.tryCreate("p1") -> ParticipantNodeConfig(
            crypto = CryptoConfig(
              provider = Kms,
              signing = SigningSchemeConfig(
                keys = CryptoSchemeConfig(
                  default = Some(signingKeySpec),
                  allowed = Some(supportedSigningKeySchemes),
                )
              ),
              kms = Some(
                KmsConfig.Aws.defaultTestConfig.copy(sessionSigningKeys =
                  SessionSigningKeysConfig.default.copy(signingKeySpec = sessionSigningKeySpec)
                )
              ),
            )
          )
        )
      )

      assertErrors(config)(
        s"The selected signing key specification, $sessionSigningKeySpec, for session signing keys is not " +
          s"supported. Supported keys are: $supportedSigningKeySchemes."
      )
    }
  }

  "nodes using authorization services" should {
    def authService(
        secret: NonEmptyString,
        scope: Option[String] = None,
        audience: Option[String] = None,
        privileged: Boolean = false,
        user: Option[AuthorizedUser] = None,
    ): AuthServiceConfig =
      AuthServiceConfig
        .UnsafeJwtHmac256(
          secret = secret,
          targetAudience = audience,
          targetScope = scope,
          privileged = privileged,
          users = user.toList,
        )
    def createConfig(auth: AuthServiceConfig*): CantonConfig =
      CantonConfig(
        participants = Map(
          InstanceName.tryCreate("p1") -> ParticipantNodeConfig(
            ledgerApi = LedgerApiServerConfig(
              authServices = auth.toList
            )
          )
        )
      )
    val (mySecret, mySecret2) =
      (NonEmptyString.tryCreate("pyjama"), NonEmptyString.tryCreate("longjohns"))
    "allow two authorization services with different scopes" in {
      val config = createConfig(
        authService(mySecret, scope = Some("scope1")),
        authService(mySecret2, scope = Some("scope2"), privileged = true),
      )
      assertValid(config)
    }
    "prevent two authorization services with the same scope" in {
      val config = createConfig(
        authService(mySecret, scope = Some("scope1")),
        authService(mySecret2, scope = Some("scope1"), privileged = true),
      )
      assertErrors(config)("Multiple authorization service configured with the same scope in p1")
    }
    "allow two authorization services with different audiences" in {
      val config = createConfig(
        authService(mySecret, audience = Some("audience1")),
        authService(mySecret2, audience = Some("audience2"), privileged = true),
      )
      assertValid(config)
    }
    "prevent two authorization services with the same audience" in {
      val config = createConfig(
        authService(mySecret, audience = Some("audience1")),
        authService(mySecret2, audience = Some("audience1"), privileged = true),
      )
      assertErrors(config)("Multiple authorization service configured with the same audience in p1")
    }

    "prevent privileged authorization service with users" in {
      val config = createConfig(
        authService(
          mySecret2,
          audience = Some("audience1"),
          privileged = true,
          user = Some(
            AuthorizedUser(
              userId = "alice",
              allowedServices = Seq.empty,
            )
          ),
        )
      )
      assertErrors(config)(
        "Authorization service cannot be configured to accept both privileged tokens and tokens for user-lists in p1"
      )
    }
  }
}
