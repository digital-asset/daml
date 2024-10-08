// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion.{
  alpha,
  beta,
  deleted,
  deprecated,
  stable,
  supported,
}
import io.circe.Encoder
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
  *    {{{lazy val v<N>: ProtocolVersionWithStatus[Alpha] = ProtocolVersion.alpha(<N>)}}}
  *
  *  - The new protocol version should be declared as alpha until it is released:
  *    Define it with type argument [[com.digitalasset.canton.version.ProtocolVersionAnnotation.Alpha]]
  *    and add it to the list in [[com.digitalasset.canton.version.ProtocolVersion.alpha]].
  *
  *  - Add a new test job for the protocol version `N` to the canton_build workflow.
  *    Make a sensible decision how often it should run.
  *    If sensible, consider to reduce the frequency some of the other protocol version test jobs are running,
  *    e.g., by moving them to the canton_nightly job.
  *
  * How to release a protocol version `N`:
  *  - Switch the type parameter of the protocol version constant `v<N>` from
  *    [[com.digitalasset.canton.version.ProtocolVersionAnnotation.Alpha]] to [[com.digitalasset.canton.version.ProtocolVersionAnnotation.Stable]]
  *    As a result, you may have to modify a couple of protobuf definitions and mark them as stable as well.
  *
  *  - Remove `v<N>` from [[com.digitalasset.canton.version.ProtocolVersion.alpha]]
  *    and add it to [[com.digitalasset.canton.buildinfo.BuildInfo.stableProtocolVersions]].
  *
  * How to release a protocol version `N` as Beta:
  *  - Switch the type parameter of the protocol version constant `v<N>` from
  *    [[com.digitalasset.canton.version.ProtocolVersionAnnotation.Alpha]] to [[com.digitalasset.canton.version.ProtocolVersionAnnotation.Beta]]
  *  - Remove `v<N>` from [[com.digitalasset.canton.version.ProtocolVersion.alpha]]
  *    and add it to [[com.digitalasset.canton.buildinfo.BuildInfo.betaProtocolVersions]].
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

  def isAlpha: Boolean = alpha.contains(this)

  def isBeta: Boolean = beta.contains(this)

  def isStable: Boolean = stable.contains(this)

  def isDeleted: Boolean = deleted.contains(this)

  def isDev: Boolean = this == ProtocolVersion.dev

  def isSupported: Boolean = supported.contains(this)

  override protected def pretty: Pretty[ProtocolVersion] =
    prettyOfString(_ => if (isDev) "dev" else v.toString)

  def toProtoPrimitive: Int = v

  def toProtoPrimitiveS: String = v.toString

  override def compare(that: ProtocolVersion): Int = v.compare(that.v)
}

object ProtocolVersion {
  type ProtocolVersionWithStatus[S <: ProtocolVersionAnnotation.Status] = ProtocolVersion {
    type Status = S
  }

  private[version] def createStable(
      v: Int
  ): ProtocolVersionWithStatus[ProtocolVersionAnnotation.Stable] =
    createWithStatus[ProtocolVersionAnnotation.Stable](v)

  private[version] def createAlpha(
      v: Int
  ): ProtocolVersionWithStatus[ProtocolVersionAnnotation.Alpha] =
    createWithStatus[ProtocolVersionAnnotation.Alpha](v)
  private[version] def createBeta(
      v: Int
  ): ProtocolVersionWithStatus[ProtocolVersionAnnotation.Beta] =
    createWithStatus[ProtocolVersionAnnotation.Beta](v)

  private def createWithStatus[S <: ProtocolVersionAnnotation.Status](
      v: Int
  ): ProtocolVersionWithStatus[S] =
    new ProtocolVersion(v) { override type Status = S }

  implicit val protocolVersionWriter: ConfigWriter[ProtocolVersion] =
    ConfigWriter.toString(_.toProtoPrimitiveS)

  lazy implicit val protocolVersionReader: ConfigReader[ProtocolVersion] =
    ConfigReader.fromString[ProtocolVersion] { str =>
      ProtocolVersion.create(str).leftMap[FailureReason](InvalidProtocolVersion.apply)
    }

  implicit val getResultProtocolVersion: GetResult[ProtocolVersion] =
    GetResult(r => ProtocolVersion(r.nextInt()))

  implicit val setParameterProtocolVersion: SetParameter[ProtocolVersion] =
    (pv: ProtocolVersion, pp: PositionedParameters) => pp >> pv.v

  implicit val protocolVersionEncoder: Encoder[ProtocolVersion] =
    Encoder.encodeString.contramap[ProtocolVersion](p => if (p.isDev) "dev" else p.v.toString)

  private[version] def unsupportedErrorMessage(
      pv: ProtocolVersion,
      includeDeleted: Boolean = false,
  ) = {
    val deleted = Option.when(includeDeleted)(ProtocolVersion.deleted.forgetNE).getOrElse(Nil)

    val supportedPVs: NonEmpty[List[String]] = (supported ++ deleted).sorted.map(_.toString)

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
  def parseUncheckedS(rawVersion: String): Either[String, ProtocolVersion] =
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

  /** Same as above when parsing a raw version value */
  def parseUnchecked(rawVersion: Int): Either[String, ProtocolVersion] =
    Right(ProtocolVersion(rawVersion))

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
    parseUncheckedS(rawVersion).flatMap { pv =>
      val isSupported = pv.isSupported || (allowDeleted && pv.isDeleted)

      Either.cond(isSupported, pv, unsupportedErrorMessage(pv, includeDeleted = allowDeleted))
    }

  /** Like [[create]] ensures a supported protocol version; but throws a runtime exception for errors.
    */
  def tryCreate(rawVersion: String): ProtocolVersion = create(rawVersion).valueOr(sys.error)

  /** Like [[create]] ensures a supported protocol version; tailored to (de-)serialization purposes.
    */
  def fromProtoPrimitive(
      rawVersion: Int,
      allowDeleted: Boolean = false,
  ): ParsingResult[ProtocolVersion] = {
    val pv = ProtocolVersion(rawVersion)
    val isSupported = pv.isSupported || (allowDeleted && pv.isDeleted)

    Either.cond(isSupported, pv, OtherError(unsupportedErrorMessage(pv)))
  }

  final case class InvalidProtocolVersion(override val description: String) extends FailureReason

  // All stable protocol versions supported by this release
  // TODO(#15561) Switch to non-empty again
  val stable: List[ProtocolVersion] =
    parseFromBuildInfo(BuildInfo.stableProtocolVersions.toSeq)

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
      ProtocolVersion(31),
    )

  val alpha: NonEmpty[List[ProtocolVersionWithStatus[ProtocolVersionAnnotation.Alpha]]] =
    NonEmpty.mk(List, ProtocolVersion.v32, ProtocolVersion.dev)

  val beta: List[ProtocolVersionWithStatus[ProtocolVersionAnnotation.Beta]] =
    parseFromBuildInfo(BuildInfo.betaProtocolVersions.toSeq)
      .map(pv => ProtocolVersion.createBeta(pv.v))

  val supported: NonEmpty[List[ProtocolVersion]] = (alpha ++ beta ++ stable).sorted

  private val allProtocolVersions = deprecated ++ deleted ++ alpha ++ beta ++ stable

  require(
    allProtocolVersions.sizeCompare(allProtocolVersions.distinct) == 0,
    s"All the protocol versions should be distinct." +
      s"Found: ${Map("deprecated" -> deprecated, "deleted" -> deleted, "alpha" -> alpha, "stable" -> stable)}",
  )

  // TODO(i15561): change back to `stableAndSupported.max1` once there is a stable Daml 3 protocol version
  val latest: ProtocolVersion = stable.lastOption.getOrElse(alpha.head1)

  lazy val dev: ProtocolVersionWithStatus[ProtocolVersionAnnotation.Alpha] =
    ProtocolVersion.createAlpha(Int.MaxValue)

  lazy val v32: ProtocolVersionWithStatus[ProtocolVersionAnnotation.Alpha] =
    ProtocolVersion.createAlpha(32)

  // Minimum stable protocol version introduced
  lazy val minimum: ProtocolVersion = v32

  private def parseFromBuildInfo(pv: Seq[String]): List[ProtocolVersion] =
    pv.map(parseUncheckedS)
      .map(_.valueOr(sys.error))
      .toList
}

/*
 This class wraps a protocol version which is global to the participant.
 The wrapped value usually corresponds to the latest (stable) protocol version supported by the binary.
 */
final case class ReleaseProtocolVersion(v: ProtocolVersion) extends AnyVal

object ReleaseProtocolVersion {
  val latest: ReleaseProtocolVersion = ReleaseProtocolVersion(ProtocolVersion.latest)
}

final case class ProtoVersion(v: Int) extends AnyVal

object ProtoVersion {
  implicit val protoVersionOrdering: Ordering[ProtoVersion] =
    Ordering.by[ProtoVersion, Int](_.v)
}
