// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.daml.error.ErrorCategory.SecurityAlert
import com.daml.error.{ErrorCode, Explanation, Resolution}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.HandshakeErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.version.ProtocolVersion.InvalidProtocolVersion
import com.digitalasset.canton.version.ProtocolVersionCompatibility.UnsupportedVersion
import pureconfig.error.FailureReason
import pureconfig.{ConfigReader, ConfigWriter}

object ProtocolVersionCompatibility {

  /** Returns the protocol versions supported by the participant of the current release.
    */
  def supportedProtocolsParticipant(
      cantonNodeParameters: CantonNodeParameters,
      release: ReleaseVersion = ReleaseVersion.current,
  ): NonEmpty[List[ProtocolVersion]] = {
    val unstableAndBeta =
      if (cantonNodeParameters.devVersionSupport && cantonNodeParameters.nonStandardConfig)
        ProtocolVersion.unstable.forgetNE ++ ReleaseVersionToProtocolVersions
          .getBetaProtocolVersions(release)
      else if (cantonNodeParameters.betaVersionSupport)
        ReleaseVersionToProtocolVersions.getBetaProtocolVersions(release)
      else List.empty

    ReleaseVersionToProtocolVersions.getOrElse(
      release,
      sys.error(
        s"Please add the supported protocol versions of a participant of release version $release to `majorMinorToProtocolVersions` in `ReleaseVersionToProtocolVersions.scala`."
      ),
    ) ++ unstableAndBeta
  }

  /** Returns the protocol versions supported by the participant of the specified release.
    * includeUnstableVersions: include unstable versions
    * includeBetaVersions: include Beta versions
    */
  def supportedProtocolsParticipant(
      includeUnstableVersions: Boolean,
      includeBetaVersions: Boolean,
      release: ReleaseVersion,
  ): NonEmpty[List[ProtocolVersion]] = {
    val beta =
      if (includeBetaVersions)
        ReleaseVersionToProtocolVersions.getBetaProtocolVersions(release)
      else List.empty

    val unstable =
      if (includeUnstableVersions)
        ProtocolVersion.unstable.forgetNE
      else List.empty

    ReleaseVersionToProtocolVersions.getOrElse(
      release,
      sys.error(
        s"Please add the supported protocol versions of a participant of release version $release to `majorMinorToProtocolVersions` in `ReleaseVersionToProtocolVersions.scala`."
      ),
    ) ++ beta ++ unstable
  }

  /** Returns the protocol versions supported by the domain of the current release.
    * Fails if no stable protocol versions are found
    */
  def trySupportedProtocolsDomain(
      cantonNodeParameters: CantonNodeParameters,
      release: ReleaseVersion = ReleaseVersion.current,
  ): NonEmpty[List[ProtocolVersion]] = {
    val unstableAndBeta =
      if (cantonNodeParameters.devVersionSupport && cantonNodeParameters.nonStandardConfig)
        ProtocolVersion.unstable.forgetNE ++ ReleaseVersionToProtocolVersions
          .getBetaProtocolVersions(release)
      else if (cantonNodeParameters.betaVersionSupport)
        ReleaseVersionToProtocolVersions.getBetaProtocolVersions(release)
      else List.empty

    ReleaseVersionToProtocolVersions.getOrElse(
      release,
      sys.error(
        s"Please add the supported protocol versions of domain nodes of release version $release to `majorMinorToProtocolVersions` in `ReleaseVersionToProtocolVersions.scala`."
      ),
    ) ++ unstableAndBeta
  }

  /** Returns the protocol versions supported by the domain of the specified release.
    * includeUnstableVersions: include unstable versions
    * includeBetaVersions: include beta versions
    */
  def trySupportedProtocolsDomain(
      includeUnstableVersions: Boolean,
      includeBetaVersions: Boolean,
      release: ReleaseVersion,
  ): NonEmpty[List[ProtocolVersion]] = {
    val beta =
      if (includeBetaVersions)
        ReleaseVersionToProtocolVersions.getBetaProtocolVersions(release)
      else List.empty

    val unstable =
      if (includeUnstableVersions)
        ProtocolVersion.unstable.forgetNE
      else List.empty

    ReleaseVersionToProtocolVersions.getOrElse(
      release,
      sys.error(
        s"Please add the supported protocol versions of domain nodes of release version $release to `majorMinorToProtocolVersions` in `ReleaseVersionToProtocolVersions.scala`."
      ),
    ) ++ beta ++ unstable
  }

  final case class UnsupportedVersion(version: ProtocolVersion, supported: Seq[ProtocolVersion])
      extends FailureReason {
    override def description: String =
      s"CantonVersion $version is not supported! The supported versions are ${supported.map(_.toString).mkString(", ")}. Please configure one of these protocol versions in the DomainParameters. "
  }

  /** Returns successfully if the client and server should be compatible.
    * Otherwise returns an error message.
    *
    * The client and server are compatible if the protocol version required by the server is not lower than
    * the clientMinimumVersion and the protocol version required by the server is among the protocol versions supported
    * by the client (exact string match).
    *
    * Note that this compatibility check cannot be implemented by simply verifying whether the supported
    * version by the client is larger than the required version by the server as this may lead to issues with
    * patches for old minor versions.
    * For example, if the latest release version is 1.3.0 but we release patch release version 1.1.1 after
    * the release of version 1.3.0, a node on version 1.3.0 which only checks whether
    * are versions are smaller, would mistakenly indicate that it is compatible with a node running version 1.1.1.
    * This issue is avoided if the client sends all protocol versions it supports and an exact string match is required.
    * Generally, this sort of error can occur because Canton is operated in a distributed environment where not every
    * node is on the same version.
    */
  def canClientConnectToServer(
      clientSupportedVersions: Seq[ProtocolVersion],
      server: ProtocolVersion,
      clientMinimumProtocolVersion: Option[ProtocolVersion],
  ): Either[HandshakeError, Unit] = {
    val clientSupportsRequiredVersion = clientSupportedVersions.contains(server)
    val clientMinVersionLargerThanReqVersion = clientMinimumProtocolVersion.exists(_ > server)
    // if dev-version support is on for participant and domain, ignore the min protocol version
    if (clientSupportsRequiredVersion && server.isUnstable)
      Right(())
    else if (clientMinVersionLargerThanReqVersion)
      Left(MinProtocolError(server, clientMinimumProtocolVersion, clientSupportsRequiredVersion))
    else if (!clientSupportsRequiredVersion)
      Left(VersionNotSupportedError(server, clientSupportedVersions))
    else Right(())
  }
}

/** Trait for errors that are returned to clients when handshake fails. */
sealed trait HandshakeError {
  def description: String
}

final case class MinProtocolError(
    server: ProtocolVersion,
    clientMinimumProtocolVersion: Option[ProtocolVersion],
    clientSupportsRequiredVersion: Boolean,
) extends HandshakeError {
  override def description: String =
    s"The version required by the domain (${server.toString}) is lower than the minimum version configured by the participant (${clientMinimumProtocolVersion
        .map(_.toString)
        .getOrElse("")}). " +
      s"${if (clientSupportsRequiredVersion) "The participant supports the version required by the domain and would be able to connect to the domain if the minimum required version is configured to be lower."} "
}

final case class VersionNotSupportedError(
    server: ProtocolVersion,
    clientSupportedVersions: Seq[ProtocolVersion],
) extends HandshakeError {
  override def description: String =
    s"The protocol version required by the server (${server.toString}) is not among the supported protocol versions by the client $clientSupportedVersions. "
}

object HandshakeErrors extends HandshakeErrorGroup {

  @Explanation(
    """This error is logged or returned if a participant or domain are using deprecated protocol versions.
      |Deprecated protocol versions might not be secure anymore."""
  )
  @Resolution(
    """Migrate to a new domain that uses the most recent protocol version."""
  )
  object DeprecatedProtocolVersion extends ErrorCode("DEPRECATED_PROTOCOL_VERSION", SecurityAlert) {
    final case class WarnSequencerClient(domainAlias: DomainAlias, version: ProtocolVersion)(
        implicit val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"This node is connecting to a sequencer using the deprecated protocol version " +
            s"${version} which should not be used in production. We recommend only connecting to sequencers with a later protocol version (such as ${ProtocolVersion.latest})."
        )
    final case class WarnDomain(name: InstanceName, version: ProtocolVersion)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          s"This domain node is configured to use the deprecated protocol version " +
            s"${version} which should not be used in production. We recommend migrating to a later protocol version (such as ${ProtocolVersion.latest})."
        )

    final case class WarnParticipant(
        name: InstanceName,
        minimumProtocolVersion: Option[ProtocolVersion],
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          s"This participant node's configured minimum protocol version ${minimumProtocolVersion} includes deprecated protocol versions. " +
            s"We recommend using only the most recent protocol versions."
        ) {
      override def logOnCreation: Boolean = false
    }
  }
}

/** Wrapper around a [[ProtocolVersion]] so we can verify during configuration loading that domain operators only
  * configure a [[ProtocolVersion]] which is supported by the corresponding sequencer release.
  */
final case class DomainProtocolVersion(version: ProtocolVersion) {
  def unwrap: ProtocolVersion = version
}
object DomainProtocolVersion {
  implicit val domainProtocolVersionWriter: ConfigWriter[DomainProtocolVersion] =
    ConfigWriter.toString(_.version.toProtoPrimitiveS)
  lazy implicit val domainProtocolVersionReader: ConfigReader[DomainProtocolVersion] = {
    ConfigReader.fromString[DomainProtocolVersion] { str =>
      for {
        version <- ProtocolVersion
          .parseUnchecked(str)
          .leftMap[FailureReason](InvalidProtocolVersion)
        _ <- Either.cond(
          // we support development versions when parsing, but catch dev versions without
          // the safety flag during config validation
          ProtocolVersionCompatibility
            .trySupportedProtocolsDomain(
              includeUnstableVersions = true,
              includeBetaVersions = true,
              release = ReleaseVersion.current,
            )
            .contains(version),
          (),
          UnsupportedVersion(
            version,
            ProtocolVersionCompatibility.trySupportedProtocolsDomain(
              includeUnstableVersions = true,
              includeBetaVersions = true,
              release = ReleaseVersion.current,
            ),
          ),
        )
      } yield DomainProtocolVersion(version)
    }
  }
}

/** Wrapper around a [[ProtocolVersion]] so we can verify during configuration loading that participant operators only
  * configure a minimum [[ProtocolVersion]] in [[com.digitalasset.canton.participant.config.LocalParticipantConfig]]
  * which is supported by the corresponding participant release.
  */
final case class ParticipantProtocolVersion(version: ProtocolVersion) {
  def unwrap: ProtocolVersion = version
}
object ParticipantProtocolVersion {
  implicit val participantProtocolVersionWriter: ConfigWriter[ParticipantProtocolVersion] =
    ConfigWriter.toString(_.version.toProtoPrimitiveS)

  lazy implicit val participantProtocolVersionReader: ConfigReader[ParticipantProtocolVersion] = {
    ConfigReader.fromString[ParticipantProtocolVersion] { str =>
      for {
        version <- ProtocolVersion
          .parseUnchecked(str)
          .leftMap[FailureReason](InvalidProtocolVersion)
        _ <- Either.cond(
          // same as domain: support parsing of dev
          ProtocolVersionCompatibility
            .supportedProtocolsParticipant(
              includeUnstableVersions = true,
              includeBetaVersions = true,
              release = ReleaseVersion.current,
            )
            .contains(version),
          (),
          UnsupportedVersion(
            version,
            ProtocolVersionCompatibility.supportedProtocolsParticipant(
              includeUnstableVersions = true,
              includeBetaVersions = true,
              release = ReleaseVersion.current,
            ),
          ),
        )
      } yield ParticipantProtocolVersion(version)
    }
  }

}
