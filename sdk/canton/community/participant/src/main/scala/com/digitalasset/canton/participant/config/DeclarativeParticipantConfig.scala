// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{
  CantonConfigValidator,
  CantonConfigValidatorInstances,
  ConfidentialConfigWriter,
  UniformCantonConfigValidation,
}
import com.digitalasset.canton.ledger.api.{IdentityProviderId, JwksUrl}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnections,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias}
import com.google.protobuf.ByteString
import pureconfig.error.CannotConvert

import java.io.File

/** Declarative participant config
  *
  * @param checkSelfConsistency
  *   if set to true (default), then after every sync operation we'll check again if it really
  *   succeeded
  * @param fetchedDarDirectory
  *   temporary directory to store the dars to
  * @param dars
  *   which dars should be uploaded
  * @param parties
  *   which parties should be allocated
  * @param removeParties
  *   if true, then any "excess party" found on the node will be deleted
  * @param idps
  *   which identity providers should be allocated
  * @param removeIdps
  *   if true, any excess idp found on the node will be deleted
  * @param users
  *   which users should be allocated
  * @param removeUsers
  *   if true, then any "excess user" found on the node will be deleted
  * @param connections
  *   which connections should be configured
  * @param removeConnections
  *   if true, then any excess connection will be disabled
  */
final case class DeclarativeParticipantConfig(
    checkSelfConsistency: Boolean = true,
    fetchedDarDirectory: File = new File("fetched-dars"),
    dars: Seq[DeclarativeDarConfig] = Seq(),
    parties: Seq[DeclarativePartyConfig] = Seq(),
    removeParties: Boolean = false,
    idps: Seq[DeclarativeIdpConfig] = Seq(),
    removeIdps: Boolean = false,
    users: Seq[DeclarativeUserConfig] = Seq(),
    removeUsers: Boolean = false,
    connections: Seq[DeclarativeConnectionConfig] = Seq(),
    removeConnections: Boolean = false,
) extends UniformCantonConfigValidation

/** Declarative dar definition
  *
  * @param location
  *   the path (or URL) to the dar or dar directory
  * @param requestHeaders
  *   optionally add additional request headers to download the dar
  * @param expectedMainPackage
  *   which package id should be expected as the main package
  */
final case class DeclarativeDarConfig(
    location: String,
    requestHeaders: Map[String, String] = Map(),
    expectedMainPackage: Option[String] = None,
) extends UniformCantonConfigValidation

sealed trait ParticipantPermissionConfig extends UniformCantonConfigValidation {
  def toNative: ParticipantPermission
}
object ParticipantPermissionConfig {

  def fromInternal(permission: ParticipantPermission): ParticipantPermissionConfig =
    permission match {
      case ParticipantPermission.Submission => Submission
      case ParticipantPermission.Confirmation => Confirmation
      case ParticipantPermission.Observation => Observation
    }

  final object Submission extends ParticipantPermissionConfig {
    val toNative: ParticipantPermission = ParticipantPermission.Submission
  }
  final object Confirmation extends ParticipantPermissionConfig {
    val toNative: ParticipantPermission = ParticipantPermission.Confirmation
  }
  final object Observation extends ParticipantPermissionConfig {
    val toNative: ParticipantPermission = ParticipantPermission.Observation
  }
}

/** Declarative party definition
  *
  * @param id
  *   the id of the party (can be prefix only which will be extended with the participant-suffix)
  * @param synchronizers
  *   if not empty, the party will be added to the selected synchronizers only, refered to by alias
  * @param permission
  *   the permission of the hosting participant
  */
final case class DeclarativePartyConfig(
    party: String,
    synchronizers: Seq[String] = Seq.empty,
    permission: ParticipantPermissionConfig = ParticipantPermissionConfig.Submission,
) extends UniformCantonConfigValidation

/** Declarative user rights definition
  *
  * @param actAs
  *   the name of the parties the user can act as. parties must exist. if they don't contain a
  *   namespace, then the participants namespace will be used
  * @param readAs
  *   the name of the parties the user can read as.
  * @param readAsAnyParty
  *   if true then the user can read as any party
  * @param participantAdmin
  *   if true then the user can act as a participant admin
  * @param identityProviderAdmin
  *   if true, then the user can act as an identity provider admin
  */
final case class DeclarativeUserRightsConfig(
    actAs: Set[String] = Set(),
    readAs: Set[String] = Set(),
    readAsAnyParty: Boolean = false,
    participantAdmin: Boolean = false,
    identityProviderAdmin: Boolean = false,
) extends UniformCantonConfigValidation

/** Declarative Idp config
  */
final case class DeclarativeIdpConfig(
    identityProviderId: String,
    isDeactivated: Boolean = false,
    jwksUrl: String,
    issuer: String,
    audience: Option[String] = None,
) extends UniformCantonConfigValidation {
  // TODO(#25043) move to config validation
  def apiIdentityProviderId: IdentityProviderId.Id =
    IdentityProviderId.Id.assertFromString(identityProviderId)
  def apiJwksUrl: JwksUrl = JwksUrl.assertFromString(jwksUrl)

}

/** Declaratively control users
  *
  * @param user
  *   the user id
  * @param primaryParty
  *   the primary party that should be used for the user
  * @param isDeactivated
  *   if true then the user is deactivatedd
  * @param annotations
  *   a property bag of annotations that can be stored alongside the user
  * @param identityProviderId
  *   the idp of the given user
  * @param rights
  *   the rights granted to the party
  */
final case class DeclarativeUserConfig(
    user: String,
    primaryParty: Option[String] = None,
    isDeactivated: Boolean = false,
    annotations: Map[String, String] = Map.empty,
    identityProviderId: String = "",
    rights: DeclarativeUserRightsConfig = DeclarativeUserRightsConfig(),
)(val resourceVersion: String = "")
    extends UniformCantonConfigValidation {

  /** map party names to namespace and filter out parties that are not yet registered
    *
    * the ledger api server needs to know the parties that we add to a user. that requires a
    * synchronizer connection. therefore, we filter here the parties that are not yet registered as
    * otherwise the ledger api server will throw errors
    */
  def mapPartiesToNamespace(
      namespace: Namespace,
      filterParty: String => Boolean,
  ): DeclarativeUserConfig = {
    def mapParty(party: String): String =
      if (party.contains(UniqueIdentifier.delimiter)) party
      else
        UniqueIdentifier.tryCreate(party, namespace).toProtoPrimitive
    copy(
      primaryParty = primaryParty.map(mapParty).filter(filterParty),
      rights = rights.copy(
        actAs = rights.actAs.map(mapParty).filter(filterParty),
        readAs = rights.readAs.map(mapParty).filter(filterParty),
      ),
    )(resourceVersion)
  }

  def needsUserChange(other: DeclarativeUserConfig): Boolean =
    primaryParty != other.primaryParty || isDeactivated != other.isDeactivated || annotations != other.annotations

}

/** Declaratively define sequencer endpoints
  *
  * @param endpoints
  *   the list of endpoints for the given sequencer. all endpoints must be of the same sequencer
  *   (same-id)
  * @param transportSecurity
  *   if true then TLS will be used
  * @param customTrustCertificates
  *   if the TLS certificate used cannot be validated against the JVMs trust store, then a trust
  *   store can be provided
  */
final case class DeclarativeSequencerConnectionConfig(
    endpoints: NonEmpty[Seq[Endpoint]],
    transportSecurity: Boolean = false,
    customTrustCertificates: Option[File] = None,
)(customTrustCertificatesFromNode: Option[ByteString] = None)
    extends UniformCantonConfigValidation {
  def customTrustCertificatesAsByteString: Either[String, Option[ByteString]] =
    customTrustCertificates
      .traverse(x => BinaryFileUtil.readByteStringFromFile(x.getPath))
      .map(_.orElse(customTrustCertificatesFromNode))

  def isEquivalent(other: DeclarativeSequencerConnectionConfig): Boolean =
    endpoints == other.endpoints && transportSecurity == other.transportSecurity && customTrustCertificatesAsByteString == other.customTrustCertificatesAsByteString

}

object DeclarativeSequencerConnectionConfig {
  def create(endpoint: Endpoint): DeclarativeSequencerConnectionConfig =
    DeclarativeSequencerConnectionConfig(endpoints = NonEmpty.mk(Seq, endpoint))()
}

/** Declarative synchronizer connection configuration
  *
  * @param synchronizerAlias
  *   the alias to refer to this connection
  * @param connections
  *   the list of sequencers with endpoints
  * @param manualConnect
  *   if true then the connection should be manual and require explicitly operator action
  * @param priority
  *   sets the priority of the connection. if a transaction can be sent to several synchronizers, it
  *   will use the one with the highest priority
  * @param initializeFromTrustedSynchronizer
  *   if true then the participant assumes that the synchronizer trust certificate of the
  *   participant is already issued
  * @param trustThreshold
  *   from how many sequencers does the node have to receive a notification to trust that it was
  *   really observed
  */
final case class DeclarativeConnectionConfig(
    synchronizerAlias: String,
    connections: NonEmpty[Map[String, DeclarativeSequencerConnectionConfig]],
    manualConnect: Boolean = false,
    priority: Int = 0,
    initializeFromTrustedSynchronizer: Boolean = false,
    trustThreshold: PositiveInt = PositiveInt.one,
) extends UniformCantonConfigValidation {

  def isEquivalent(other: DeclarativeConnectionConfig): Boolean = {
    val areConnectionsEquivalent = connections.keySet == other.connections.keySet &&
      connections.forall { case (name, conn) =>
        other.connections.get(name).exists(_.isEquivalent(conn))
      }

    if (areConnectionsEquivalent)
      this.copy(connections = other.connections) == other
    else false
  }

  def toSynchronizerConnectionConfig: Either[String, SynchronizerConnectionConfig] = {
    val sequencerConnectionsE = SequencerConnections
      .many(
        connections = connections.map { case (alias, conn) =>
          GrpcSequencerConnection(
            endpoints = conn.endpoints,
            transportSecurity = conn.transportSecurity,
            sequencerAlias = SequencerAlias.tryCreate(alias),
            customTrustCertificates = conn.customTrustCertificatesAsByteString.toOption.flatten,
          )
        }.toSeq,
        sequencerTrustThreshold = trustThreshold,
        submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
      )

    sequencerConnectionsE.map { sequencerConnections =>
      SynchronizerConnectionConfig(
        synchronizerAlias = SynchronizerAlias.tryCreate(synchronizerAlias),
        sequencerConnections = sequencerConnections,
        manualConnect = manualConnect,
        priority = priority,
        initializeFromTrustedSynchronizer = initializeFromTrustedSynchronizer,
      )
    }

  }

}

object DeclarativeConnectionConfig {
  def tryCreate(
      alias: String,
      independentEndpoints: Map[String, Endpoint],
  ): DeclarativeConnectionConfig =
    DeclarativeConnectionConfig(
      synchronizerAlias = alias,
      connections = NonEmpty
        .from(
          independentEndpoints.map { case (name, endpoint) =>
            name -> DeclarativeSequencerConnectionConfig.create(endpoint)
          }
        )
        .getOrElse(throw new IllegalArgumentException("Empty connections")),
    )
}

object DeclarativeParticipantConfig {

  import CantonConfigValidatorInstances.*
  lazy implicit val declarativeDarConfigCantonConfigValidator
      : CantonConfigValidator[DeclarativeDarConfig] =
    CantonConfigValidatorDerivation[DeclarativeDarConfig]
  lazy implicit val declarativeUserRightsConfig
      : CantonConfigValidator[DeclarativeUserRightsConfig] =
    CantonConfigValidatorDerivation[DeclarativeUserRightsConfig]
  lazy implicit val declarativeUserConfigCantonConfigValidator
      : CantonConfigValidator[DeclarativeUserConfig] =
    CantonConfigValidatorDerivation[DeclarativeUserConfig]
  lazy implicit val declarativeIdpConfigCantonConfigValidator
      : CantonConfigValidator[DeclarativeIdpConfig] =
    CantonConfigValidatorDerivation[DeclarativeIdpConfig]
  lazy implicit val declarativeParticipantPermissionConfigValidator
      : CantonConfigValidator[ParticipantPermissionConfig] =
    CantonConfigValidatorDerivation[ParticipantPermissionConfig]
  lazy implicit val declarativePartyConfigCantonConfigValidator
      : CantonConfigValidator[DeclarativePartyConfig] =
    CantonConfigValidatorDerivation[DeclarativePartyConfig]
  lazy implicit val declarativeSequencerConnectionConfigValidator
      : CantonConfigValidator[DeclarativeSequencerConnectionConfig] =
    CantonConfigValidatorDerivation[DeclarativeSequencerConnectionConfig]
  lazy implicit val declarativeConnectionConfigValidator
      : CantonConfigValidator[DeclarativeConnectionConfig] =
    CantonConfigValidatorDerivation[DeclarativeConnectionConfig]

  lazy implicit val declarativeParticipantCantonConfigValidator
      : CantonConfigValidator[DeclarativeParticipantConfig] =
    CantonConfigValidatorDerivation[DeclarativeParticipantConfig]

  object Readers {
    import com.daml.nonempty.NonEmptyUtil.instances.*
    import pureconfig.ConfigReader
    import pureconfig.generic.semiauto.*
    // import canton config to include the implicit that prevents unknown keys

    implicit val synchronizerAliasReader: ConfigReader[SynchronizerAlias] =
      ConfigReader[String255].map(SynchronizerAlias(_))

    implicit val declarativeParticipantConfigReader: ConfigReader[DeclarativeParticipantConfig] = {
      implicit val darConfigReader: ConfigReader[DeclarativeDarConfig] =
        deriveReader[DeclarativeDarConfig]

      implicit val permissionReader: ConfigReader[ParticipantPermissionConfig] = {
        implicit val submissionReader: ConfigReader[ParticipantPermissionConfig.Submission.type] =
          deriveReader[ParticipantPermissionConfig.Submission.type]
        implicit val confirmationReader
            : ConfigReader[ParticipantPermissionConfig.Confirmation.type] =
          deriveReader[ParticipantPermissionConfig.Confirmation.type]
        implicit val observerationReader
            : ConfigReader[ParticipantPermissionConfig.Observation.type] =
          deriveReader[ParticipantPermissionConfig.Observation.type]
        deriveReader[ParticipantPermissionConfig]
      }

      implicit val partyConfigReader: ConfigReader[DeclarativePartyConfig] =
        deriveReader[DeclarativePartyConfig]
      implicit val rightsConfigReader: ConfigReader[DeclarativeUserRightsConfig] =
        deriveReader[DeclarativeUserRightsConfig]
      implicit val userConfigReader: ConfigReader[DeclarativeUserConfig] =
        deriveReader[DeclarativeUserConfig]
      implicit val endpointReader: ConfigReader[Endpoint] = deriveReader[Endpoint]
      implicit val sequencerConnectionConfigReader
          : ConfigReader[DeclarativeSequencerConnectionConfig] =
        deriveReader[DeclarativeSequencerConnectionConfig].emap { parsed =>
          parsed.customTrustCertificatesAsByteString
            .leftMap(err => CannotConvert(parsed.customTrustCertificates.toString, "bytes", err))
            .map(_ => parsed)
        }
      implicit val idpConfigReader: ConfigReader[DeclarativeIdpConfig] =
        deriveReader[DeclarativeIdpConfig]
      implicit val connectionConfigReader: ConfigReader[DeclarativeConnectionConfig] =
        deriveReader[DeclarativeConnectionConfig]
      deriveReader[DeclarativeParticipantConfig]
    }

  }

  class ConfigWriters(confidential: Boolean) {
    import com.daml.nonempty.NonEmptyUtil.instances.*
    import pureconfig.ConfigWriter
    import pureconfig.generic.semiauto.*

    val confidentialWriter = new ConfidentialConfigWriter(confidential)

    implicit val synchronizerAliasReader: ConfigWriter[SynchronizerAlias] =
      ConfigWriter.toString(_.unwrap)

    implicit val declarativeParticipantConfigWriter: ConfigWriter[DeclarativeParticipantConfig] = {
      implicit val darConfigWriter: ConfigWriter[DeclarativeDarConfig] =
        confidentialWriter[DeclarativeDarConfig](c =>
          c.copy(requestHeaders = c.requestHeaders.map { case (k, v) => (k, "*****") })
        )

      implicit val permissionWriter: ConfigWriter[ParticipantPermissionConfig] = {
        implicit val submissionWriter: ConfigWriter[ParticipantPermissionConfig.Submission.type] =
          deriveWriter[ParticipantPermissionConfig.Submission.type]
        implicit val confirmationWriter
            : ConfigWriter[ParticipantPermissionConfig.Confirmation.type] =
          deriveWriter[ParticipantPermissionConfig.Confirmation.type]
        implicit val observerationWriter
            : ConfigWriter[ParticipantPermissionConfig.Observation.type] =
          deriveWriter[ParticipantPermissionConfig.Observation.type]
        deriveWriter[ParticipantPermissionConfig]
      }

      implicit val partyConfigWriter: ConfigWriter[DeclarativePartyConfig] =
        deriveWriter[DeclarativePartyConfig]
      implicit val rightsConfigWriter: ConfigWriter[DeclarativeUserRightsConfig] =
        deriveWriter[DeclarativeUserRightsConfig]
      implicit val userConfigWriter: ConfigWriter[DeclarativeUserConfig] =
        deriveWriter[DeclarativeUserConfig]
      implicit val endpointReader: ConfigWriter[Endpoint] = deriveWriter[Endpoint]
      implicit val sequencerConnectionConfigWriter
          : ConfigWriter[DeclarativeSequencerConnectionConfig] =
        deriveWriter[DeclarativeSequencerConnectionConfig]

      implicit val idpConfigWriter: ConfigWriter[DeclarativeIdpConfig] =
        deriveWriter[DeclarativeIdpConfig]
      implicit val connectionConfigWriter: ConfigWriter[DeclarativeConnectionConfig] =
        deriveWriter[DeclarativeConnectionConfig]
      deriveWriter[DeclarativeParticipantConfig]
    }

  }

}
