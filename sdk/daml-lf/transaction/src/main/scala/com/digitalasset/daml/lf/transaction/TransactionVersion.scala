// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.daml.nameof.NameOf
import com.digitalasset.daml.lf.language.LanguageVersion

/** Currently supported versions of the Daml-LF transaction specification.
  */
object TransactionVersion {

  private[lf] val All: List[TransactionVersion] = LanguageVersion.AllV2

  private[this] val fromStringMapping = Map(
    "2.1" -> LanguageVersion.v2_1,
    "dev" -> LanguageVersion.v2_dev,
  )

  private[this] val fromIntMapping = Map(
    1 -> LanguageVersion.v2_1,
    Int.MaxValue -> LanguageVersion.v1_dev,
  )

  private[this] val toStringMapping = fromStringMapping.map { case (k, v) => v -> k }

  private[this] val toIntMapping = fromIntMapping.map { case (k, v) => v -> k }

  private[lf] def fromString(vs: String): Either[String, TransactionVersion] =
    fromStringMapping.get(vs).toRight(s"Unsupported transaction version '$vs'")

  private[lf] def fromInt(i: Int): Either[String, TransactionVersion] =
    fromIntMapping.get(i).toRight(s"Unsupported transaction version '$i'")

  private[digitalasset] def toProtoValue(ver: LanguageVersion): String =
    toStringMapping
      .get(ver)
      .getOrElse(
        InternalError.illegalArgumentException(
          NameOf.qualifiedNameOfCurrentFunc,
          s"Internal Error: unexpected language version $ver",
        )
      )

  private[lf] def toInt(ver: LanguageVersion): Int =
    toIntMapping
      .get(ver)
      .getOrElse(
        InternalError.illegalArgumentException(
          NameOf.qualifiedNameOfCurrentFunc,
          s"Internal Error: unexpected language version $ver",
        )
      )

  val minVersion: TransactionVersion = All.min
  val maxVersion: TransactionVersion = All.max

  private[lf] val minContractKeys = LanguageVersion.Features.contractKeys

  private[lf] val minTextMap = LanguageVersion.Features.textMap

  private[lf] val minChoiceAuthorizers = LanguageVersion.Features.choiceAuthority

  private[lf] val minPackageVersion = LanguageVersion.Features.persistedPackageVersion

  val VDev = LanguageVersion.v2_dev

  private[lf] def txVersion(tx: Transaction) = {
    import scala.Ordering.Implicits._
    tx.nodes.valuesIterator.foldLeft(TransactionVersion.minVersion) {
      case (acc, action: Node.Action) => acc max action.version
      case (acc, _: Node.Rollback) => acc
    }
  }

  private[lf] def asVersionedTransaction(
      tx: Transaction
  ): VersionedTransaction =
    VersionedTransaction(txVersion(tx), tx.nodes, tx.roots)

  val StableVersions: VersionRange[TransactionVersion] =
    LanguageVersion.StableVersions(LanguageVersion.Major.V2)

  private[lf] val DevVersions: VersionRange[TransactionVersion] =
    LanguageVersion.AllVersions(LanguageVersion.default.major)

}
