// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.daml.error.ErrorCategory.SecurityAlert
import com.daml.error.{ErrorCode, Explanation, Resolution}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
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

  /** Returns the protocol versions supported by the canton node parameters and the release.
    *
    * @param release defaults to the current release
    */
  def supportedProtocols(
      cantonNodeParameters: CantonNodeParameters,
      release: ReleaseVersion = ReleaseVersion.current,
  ): NonEmpty[List[ProtocolVersion]] = {
    val unstableAndBeta =
      if (cantonNodeParameters.alphaVersionSupport && cantonNodeParameters.nonStandardConfig)
        ProtocolVersion.alpha.forgetNE ++ ReleaseVersionToProtocolVersions
          .getBetaProtocolVersions(release)
      else if (cantonNodeParameters.betaVersionSupport)
        ReleaseVersionToProtocolVersions.getBetaProtocolVersions(release)
      else List.empty

    val supportedPVs = ReleaseVersionToProtocolVersions.getOrElse(
      release,
      sys.error(
        s"Please review the supported protocol versions of release version $release in `ReleaseVersionToProtocolVersions.scala`."
      ),
    ) ++ unstableAndBeta

    // If the release contains an unstable, alpha or beta protocol version, it is mentioned twice in the result
    supportedPVs.distinct
  }

  /** Returns the protocol versions supported by the release.
    */
  def supportedProtocols(
      includeAlphaVersions: Boolean,
      includeBetaVersions: Boolean,
      release: ReleaseVersion,
  ): NonEmpty[List[ProtocolVersion]] = {
    val beta =
      if (includeBetaVersions)
        ReleaseVersionToProtocolVersions.getBetaProtocolVersions(release)
      else List.empty

    val alpha =
      if (includeAlphaVersions)
        ProtocolVersion.alpha.forgetNE
      else List.empty

    val supportedPVs = ReleaseVersionToProtocolVersions.getOrElse(
      release,
      sys.error(
        s"Please review the supported protocol versions of release version $release in `ReleaseVersionToProtocolVersions.scala`."
      ),
    ) ++ beta ++ alpha

    // If the release contains an unstable, alpha or beta protocol version, it is mentioned twice in the result
    supportedPVs.distinct
  }

  final case class UnsupportedVersion(version: ProtocolVersion, supported: Seq[ProtocolVersion])
      extends FailureReason {
    override def description: String =
      s"CantonVersion $version is not supported! The supported versions are ${supported.map(_.toString).mkString(", ")}. Please configure one of these protocol versions in the SynchronizerParameters. "
  }

  /** Returns successfully if the client and server should be compatible.
    * Otherwise returns an error message.
    *
    * The client and server are compatible if both of the following conditions are true:
    *   - The protocol version required by the server is among the protocol versions supported by the client.
    *   - The protocol version required by the server is not lower than `clientMinimumVersion`.
    *
    * Note that the second condition is not enforced if support for development versions is active for both
    * client and server.
    */
  def canClientConnectToServer(
      clientSupportedVersions: Seq[ProtocolVersion],
      serverVersion: ProtocolVersion,
      clientMinimumVersion: Option[ProtocolVersion],
  ): Either[HandshakeError, Unit] = {
    val clientSupportsRequiredVersion = clientSupportedVersions
      .filter(clientVersion => clientMinimumVersion.forall(_ <= clientVersion))
      .contains(serverVersion)

    val clientMinVersionLargerThanReqVersion = clientMinimumVersion.exists(_ > serverVersion)

    // if dev-version support is on for participant and synchronizer, ignore the min protocol version
    if (clientSupportsRequiredVersion && serverVersion.isAlpha)
      Either.unit
    else if (clientMinVersionLargerThanReqVersion)
      Left(MinProtocolError(serverVersion, clientMinimumVersion, clientSupportsRequiredVersion))
    else if (!clientSupportsRequiredVersion)
      Left(VersionNotSupportedError(serverVersion, clientSupportedVersions))
    else Either.unit
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
    s"The version required by the synchronizer (${server.toString}) is lower than the minimum version configured by the participant (${clientMinimumProtocolVersion
        .map(_.toString)
        .getOrElse("")}). " +
      s"${if (clientSupportsRequiredVersion) "The participant supports the version required by the synchronizer and would be able to connect to the synchronizer if the minimum required version is configured to be lower."} "
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
    """This error is logged or returned if a participant or synchronizer are using deprecated protocol versions.
      |Deprecated protocol versions might not be secure anymore."""
  )
  @Resolution(
    """Migrate to a new synchronizer that uses the most recent protocol version."""
  )
  object DeprecatedProtocolVersion extends ErrorCode("DEPRECATED_PROTOCOL_VERSION", SecurityAlert) {
    final case class WarnSequencerClient(
        synchronizerAlias: SynchronizerAlias,
        version: ProtocolVersion,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"This node is connecting to a sequencer using the deprecated protocol version " +
            s"$version which should not be used in production. We recommend only connecting to sequencers with a later protocol version (such as ${ProtocolVersion.latest})."
        )

    final case class WarnParticipant(
        name: InstanceName,
        minimumProtocolVersion: Option[ProtocolVersion],
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          s"This participant node's configured minimum protocol version $minimumProtocolVersion includes deprecated protocol versions. " +
            s"We recommend using only the most recent protocol versions."
        ) {
      override def logOnCreation: Boolean = false
    }
  }
}

/** Wrapper around a [[ProtocolVersion]] so we can verify during configuration loading that synchronizer operators only
  * configure a [[ProtocolVersion]] which is supported by the corresponding sequencer release.
  */
final case class SynchronizerProtocolVersion(version: ProtocolVersion) {
  def unwrap: ProtocolVersion = version
}
object SynchronizerProtocolVersion {
  implicit val synchronizerProtocolVersionWriter: ConfigWriter[SynchronizerProtocolVersion] =
    ConfigWriter.toString(_.version.toProtoPrimitiveS)
  lazy implicit val synchronizerProtocolVersionReader: ConfigReader[SynchronizerProtocolVersion] =
    ConfigReader.fromString[SynchronizerProtocolVersion] { str =>
      for {
        version <- ProtocolVersion
          .parseUncheckedS(str)
          .leftMap[FailureReason](InvalidProtocolVersion.apply)
        _ <- Either.cond(
          // we support development versions when parsing, but catch dev versions without
          // the safety flag during config validation
          ProtocolVersionCompatibility
            .supportedProtocols(
              includeAlphaVersions = true,
              includeBetaVersions = true,
              release = ReleaseVersion.current,
            )
            .contains(version),
          (),
          UnsupportedVersion(
            version,
            ProtocolVersionCompatibility.supportedProtocols(
              includeAlphaVersions = true,
              includeBetaVersions = true,
              release = ReleaseVersion.current,
            ),
          ),
        )
      } yield SynchronizerProtocolVersion(version)
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

  lazy implicit val participantProtocolVersionReader: ConfigReader[ParticipantProtocolVersion] =
    ConfigReader.fromString[ParticipantProtocolVersion] { str =>
      for {
        version <- ProtocolVersion
          .parseUncheckedS(str)
          .leftMap[FailureReason](InvalidProtocolVersion.apply)
        _ <- Either.cond(
          // same as synchronizer: support parsing of dev
          ProtocolVersionCompatibility
            .supportedProtocols(
              includeAlphaVersions = true,
              includeBetaVersions = true,
              release = ReleaseVersion.current,
            )
            .contains(version),
          (),
          UnsupportedVersion(
            version,
            ProtocolVersionCompatibility.supportedProtocols(
              includeAlphaVersions = true,
              includeBetaVersions = true,
              release = ReleaseVersion.current,
            ),
          ),
        )
      } yield ParticipantProtocolVersion(version)
    }

}
