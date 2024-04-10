// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion.{deleted, deprecated, supported, unstable}
import pureconfig.error.FailureReason
import pureconfig.{ConfigReader, ConfigWriter}
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

/** A Canton protocol version is a snapshot of how the Canton protocols, that nodes use to communicate, function at a certain point in time
  * (e.g., this ‘snapshot’ contains the information what exactly a `SubmissionRequest` to the sequencer looks like and how exactly a Sequencer
  * handles a call of the `SendAsync` RPC).
  * It is supposed to capture everything that is involved in two different Canton nodes interacting with each other.
  *
  * The protocol version is important for ensuring we meet our compatibility guarantees such that we can
  *  - update systems running older Canton versions
  *  - migrate data from older versions in the database
  *  - communicate with Canton nodes of different releases
  *
  * Two Canton nodes can interact if they can speak the same protocol version.
  *
  * For more details, please refer to the [[https://docs.daml.com/canton/usermanual/versioning.html versioning documentation]]
  * in the user manual.
  *
  * How to add a new protocol version `N`:
  *  - Define a new constant `v<N>` in the [[ProtocolVersion$]] object via
  *    {{{lazy val v<N>: ProtocolVersionWithStatus[Unstable] = ProtocolVersion.unstable(<N>)}}}
  *
  *  - The new protocol version should be declared as unstable until it is released:
  *    Define it with type argument [[com.digitalasset.canton.version.ProtocolVersionAnnotation.Unstable]]
  *    and add it to the list in [[com.digitalasset.canton.version.ProtocolVersion.unstable]].
  *
  *  - Add a new test job for the protocol version `N` to the canton_build workflow.
  *    Make a sensible decision how often it should run.
  *    If sensible, consider to reduce the frequency some of the other protocol version test jobs are running,
  *    e.g., by moving them to the canton_nightly job.
  *
  * How to release a protocol version `N`:
  *  - Switch the type parameter of the protocol version constant `v<N>` from
  *    [[com.digitalasset.canton.version.ProtocolVersionAnnotation.Unstable]] to [[com.digitalasset.canton.version.ProtocolVersionAnnotation.Stable]]
  *    As a result, you may have to modify a couple of protobuf definitions and mark them as stable as well.
  *
  *  - Remove `v<N>` from [[com.digitalasset.canton.version.ProtocolVersion.unstable]]
  *    and add it to [[com.digitalasset.canton.buildinfo.BuildInfo.protocolVersions]].
  *
  *  - Check the test jobs for protocol versions:
  *    Likely `N` will become the default protocol version used by the `test` job,
  *    namely [[com.digitalasset.canton.version.ProtocolVersion.latest]].
  *    So the separate test job for `N` is no longer needed.
  *    Conversely, we now need a new job for the previous default protocol version.
  *    Usually, it is enough to run the previous version only in canton_nightly.
  */
// Internal only: for the full background, please refer to the following [design doc](https://docs.google.com/document/d/1kDiN-373bZOWploDrtOJ69m_0nKFu_23RNzmEXQOFc8/edit?usp=sharing).
// or [code walkthrough](https://drive.google.com/file/d/199wHq-P5pVPkitu_AYLR4V3i0fJtYRPg/view?usp=sharing)
sealed case class ProtocolVersion private[version] (v: Int)
    extends Ordered[ProtocolVersion]
    with PrettyPrinting {
  type Status <: ProtocolVersionAnnotation.Status

  def isDeprecated: Boolean = deprecated.contains(this)

  def isUnstable: Boolean = unstable.contains(this)
  def isStable: Boolean = !isUnstable

  def isDeleted: Boolean = deleted.contains(this)

  def isDev: Boolean = this == ProtocolVersion.dev

  def isSupported: Boolean = supported.contains(this)

  override def pretty: Pretty[ProtocolVersion] =
    prettyOfString(_ => if (isDev) "dev" else v.toString)

  def toProtoPrimitive: Int = v

  def toProtoPrimitiveS: String = v.toString

  override def compare(that: ProtocolVersion): Int = v.compare(that.v)
}

object ProtocolVersion {
  type ProtocolVersionWithStatus[S <: ProtocolVersionAnnotation.Status] = ProtocolVersion {
    type Status = S
  }

  private[version] def stable(v: Int): ProtocolVersionWithStatus[ProtocolVersionAnnotation.Stable] =
    createWithStatus[ProtocolVersionAnnotation.Stable](v)
  private[version] def unstable(
      v: Int
  ): ProtocolVersionWithStatus[ProtocolVersionAnnotation.Unstable] =
    createWithStatus[ProtocolVersionAnnotation.Unstable](v)

  private def createWithStatus[S <: ProtocolVersionAnnotation.Status](
      v: Int
  ): ProtocolVersionWithStatus[S] =
    new ProtocolVersion(v) { override type Status = S }

  implicit val protocolVersionWriter: ConfigWriter[ProtocolVersion] =
    ConfigWriter.toString(_.toProtoPrimitiveS)

  lazy implicit val protocolVersionReader: ConfigReader[ProtocolVersion] = {
    ConfigReader.fromString[ProtocolVersion] { str =>
      ProtocolVersion.create(str).leftMap[FailureReason](InvalidProtocolVersion)
    }
  }

  implicit val getResultProtocolVersion: GetResult[ProtocolVersion] =
    GetResult { r => ProtocolVersion(r.nextInt()) }

  implicit val setParameterProtocolVersion: SetParameter[ProtocolVersion] =
    (pv: ProtocolVersion, pp: PositionedParameters) => pp >> pv.v

  private[version] def unsupportedErrorMessage(
      pv: ProtocolVersion,
      includeDeleted: Boolean = false,
  ) = {
    val supportedStablePVs = stableAndSupported.map(_.toString)

    val supportedPVs = if (includeDeleted) {
      val deletedPVs = deleted.map(pv => s"(${pv.toString})")
      supportedStablePVs ++ deletedPVs
    } else supportedStablePVs

    s"Protocol version $pv is not supported. The supported versions are ${supportedPVs.mkString(", ")}."
  }

  /** Parse a given raw version string into a [[ProtocolVersion]] without any further validation, i.e. it allows to
    * create invalid and unsupported [[ProtocolVersion]]!
    *
    * ONLY use this method when
    * - implementing functionality for the [[ProtocolVersion]] itself
    * - additional validation is being applied on the resulting [[ProtocolVersion]] afterwards as a exception
    * - testing and having a need for an invalid or unsupported [[ProtocolVersion]]
    *
    * Otherwise, use one of the other factory methods.
    */
  private[version] def parseUnchecked(rawVersion: String): Either[String, ProtocolVersion] = {
    rawVersion.toIntOption match {
      case Some(value) => Right(ProtocolVersion(value))

      case None =>
        Option
          .when(rawVersion.toLowerCase() == "dev")(ProtocolVersion.dev)
          .map(Right(_))
          .getOrElse {
            Left(s"Unable to convert string `$rawVersion` to a protocol version.")
          }
    }
  }

  /** Creates a [[ProtocolVersion]] from the given raw version value and ensures that it is a supported version.
    * @param rawVersion   String to be parsed.
    * @param allowDeleted If true, don't fail if `rawVersion` corresponds to a deleted protocol version.
    *                     This should only be used when parsing a version that does not correspond to the one
    *                     running on the domain. One such example is the minimum supported protocol version from
    *                     a participant.
    * @return
    */
  def create(
      rawVersion: String,
      allowDeleted: Boolean = false,
  ): Either[String, ProtocolVersion] =
    parseUnchecked(rawVersion).flatMap { pv =>
      val isSupported = pv.isSupported || (allowDeleted && pv.isDeleted)

      Either.cond(isSupported, pv, unsupportedErrorMessage(pv, includeDeleted = allowDeleted))
    }

  /** Like [[create]] ensures a supported protocol version; but throws a runtime exception for errors.
    */
  def tryCreate(rawVersion: String): ProtocolVersion = create(rawVersion).valueOr(sys.error)

  /** Like [[create]] ensures a supported protocol version; tailored to (de-)serialization purposes.
    */
  def fromProtoPrimitive(rawVersion: Int): ParsingResult[ProtocolVersion] = {
    val pv = ProtocolVersion(rawVersion)
    Either.cond(pv.isSupported, pv, OtherError(unsupportedErrorMessage(pv)))
  }

  /** Like [[create]] ensures a supported protocol version; tailored to (de-)serialization purposes.
    * For handshake, we want to use a string as the primitive type and not an int because that is
    * an endpoint that should never change. Using string allows us to evolve the scheme if needed.
    */
  def fromProtoPrimitiveHandshake(rawVersion: String): ParsingResult[ProtocolVersion] = {
    ProtocolVersion.create(rawVersion).leftMap(OtherError)
  }

  final case class InvalidProtocolVersion(override val description: String) extends FailureReason

  // All stable protocol versions supported by this release
  // TODO(#15561) Switch to non-empty again
  val stableAndSupported: List[ProtocolVersion] =
    BuildInfo.protocolVersions
      .map(parseUnchecked)
      .map(_.valueOr(sys.error))
      .toList

  private val deprecated: Seq[ProtocolVersion] = Seq()
  private val deleted: NonEmpty[Seq[ProtocolVersion]] =
    NonEmpty(
      Seq,
      ProtocolVersion(2),
      ProtocolVersion(3),
      ProtocolVersion(4),
      ProtocolVersion(5),
      ProtocolVersion(6),
      ProtocolVersion(30),
    )

  val unstable: NonEmpty[List[ProtocolVersionWithStatus[ProtocolVersionAnnotation.Unstable]]] =
    NonEmpty.mk(List, ProtocolVersion.v31, ProtocolVersion.dev)

  val supported: NonEmpty[List[ProtocolVersion]] = (unstable ++ stableAndSupported).sorted

  // TODO(i15561): change back to `stableAndSupported.max1` once there is a stable Daml 3 protocol version
  val latest: ProtocolVersion = stableAndSupported.lastOption.getOrElse(unstable.head1)

  lazy val dev: ProtocolVersionWithStatus[ProtocolVersionAnnotation.Unstable] =
    ProtocolVersion.unstable(Int.MaxValue)

  lazy val v31: ProtocolVersionWithStatus[ProtocolVersionAnnotation.Unstable] =
    ProtocolVersion.unstable(30)

  // Minimum stable protocol version introduced
  lazy val minimum: ProtocolVersion = v31
}

/*
 This class wraps a protocol version which is global to the participant.
 The wrapped value usually corresponds to the latest (stable) protocol version supported by the binary.
 */
final case class ReleaseProtocolVersion(v: ProtocolVersion) extends AnyVal

object ReleaseProtocolVersion {
  val latest: ReleaseProtocolVersion = ReleaseProtocolVersion(ProtocolVersion.latest)
}

object Transfer {

  /** When dealing with transfer, allow to be more precise with respect to the domain */
  final case class SourceProtocolVersion(v: ProtocolVersion) extends AnyVal

  object SourceProtocolVersion {
    implicit val getResultSourceProtocolVersion: GetResult[SourceProtocolVersion] =
      GetResult[ProtocolVersion].andThen(SourceProtocolVersion(_))

    implicit val setParameterSourceProtocolVersion: SetParameter[SourceProtocolVersion] =
      (pv: SourceProtocolVersion, pp: PositionedParameters) => pp >> pv.v
  }

  final case class TargetProtocolVersion(v: ProtocolVersion) extends AnyVal

  object TargetProtocolVersion {
    implicit val getResultTargetProtocolVersion: GetResult[TargetProtocolVersion] =
      GetResult[ProtocolVersion].andThen(TargetProtocolVersion(_))

    implicit val setParameterTargetProtocolVersion: SetParameter[TargetProtocolVersion] =
      (pv: TargetProtocolVersion, pp: PositionedParameters) => pp >> pv.v
  }
}

final case class ProtoVersion(v: Int) extends AnyVal

object ProtoVersion {
  implicit val protoVersionOrdering: Ordering[ProtoVersion] =
    Ordering.by[ProtoVersion, Int](_.v)
}
