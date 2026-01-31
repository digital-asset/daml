// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.daml.nameof.NameOf
import com.digitalasset.daml.lf.language.LanguageVersion

sealed abstract class SerializationVersion(private val idx: Int) extends Serializable with Product {
  def pretty = productPrefix
}

/** Currently supported versions of the Daml-LF transaction specification.
  */
object SerializationVersion {

  case object V1 extends SerializationVersion(1)
  case object VDev extends SerializationVersion(Int.MaxValue)

  implicit val `SerializationVersion Ordering`: Ordering[SerializationVersion] =
    Ordering.by(_.idx)

  private[lf] val All: List[SerializationVersion] = List(V1, VDev)

  private[this] val fromStringMapping = Map(
    "2.1" -> V1,
    "dev" -> VDev,
  )

  private[lf] def assign(lv: LanguageVersion):  SerializationVersion = lv.major match {
    case LanguageVersion.Major.V2 =>
      if (lv.minor.isDevVersion) VDev else V1
    case _ => throw new IllegalArgumentException(
      s"Mapping failed: LanguageVersion '${lv.pretty}' is not supported by SerializationVersion."
    )
  }

  private[this] val fromIntMapping = All.view.map(v => v.idx -> v).toMap

  private[this] val toStringMapping = fromStringMapping.map { case (k, v) => v -> k }

  private[this] val toIntMapping = fromIntMapping.map { case (k, v) => v -> k }

  def fromString(vs: String): Either[String, SerializationVersion] =
    fromStringMapping.get(vs).toRight(s"Unsupported serialization version '$vs'")

  private[lf] def fromInt(i: Int): Either[String, SerializationVersion] =
    fromIntMapping.get(i).toRight(s"Unsupported serialization version '$i'")

  private[digitalasset] def toProtoValue(ver: SerializationVersion): String =
    toStringMapping
      .get(ver)
      .getOrElse(
        InternalError.illegalArgumentException(
          NameOf.qualifiedNameOfCurrentFunc,
          s"Internal Error: unexpected language version $ver",
        )
      )

  private[lf] def toInt(ver: SerializationVersion): Int =
    toIntMapping
      .get(ver)
      .getOrElse(
        InternalError.illegalArgumentException(
          NameOf.qualifiedNameOfCurrentFunc,
          s"Internal Error: unexpected language version $ver",
        )
      )

  val minVersion: SerializationVersion = All.min
  val maxVersion: SerializationVersion = All.max

  // TODO https://github.com/digital-asset/daml/issues/22365 adopt ranges more thoroughly
  private[lf] val minContractKeys: SerializationVersion = assign(
    LanguageVersion.featureContractKeys.versionRange.min
  )

  // TODO https://github.com/digital-asset/daml/issues/22365 adopt ranges more thoroughly
  private[lf] val minChoiceAuthorizers = assign(
    LanguageVersion.featureChoiceAuthority.versionRange.min
  )

  private[lf] def txVersion(tx: Transaction): SerializationVersion = {
    import scala.Ordering.Implicits._
    tx.nodes.valuesIterator.foldLeft(SerializationVersion.minVersion) {
      case (acc, action: Node.Action) => acc max action.version
      case (acc, _: Node.Rollback) => acc
    }
  }

  private[lf] def asVersionedTransaction(
      tx: Transaction
  ): VersionedTransaction =
    VersionedTransaction(txVersion(tx), tx.nodes, tx.roots)

  val StableVersions: VersionRange.Inclusive[SerializationVersion] =
    LanguageVersion.stableLfVersionsRange.map(assign)

  private[lf] val DevVersions: VersionRange.Inclusive[SerializationVersion] =
    LanguageVersion.allLfVersionsRange.map(assign)

}
