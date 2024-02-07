// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}

sealed abstract class TransactionVersion private (
    val protoValue: String,
    private[transaction] val index: Int,
) extends Product
    with Serializable

/** Currently supported versions of the Daml-LF transaction specification.
  */
object TransactionVersion {

  // TODO(https://github.com/digital-asset/daml/issues/18240): delete V14 and V15 once canton stops
  //  mentioning them.
  case object V14 extends TransactionVersion("14", 14)
  case object V15 extends TransactionVersion("15", 15)

  case object V31 extends TransactionVersion("301", 301)
  case object VDev extends TransactionVersion("3dev", Int.MaxValue)

  val All = List(V14, V15, V31, VDev)

  implicit val Ordering: scala.Ordering[TransactionVersion] =
    scala.Ordering.by(_.index)

  private[this] val stringMapping = All.view.map(v => v.protoValue -> v).toMap

  private[this] val intMapping = All.view.map(v => v.index -> v).toMap

  def fromString(vs: String): Either[String, TransactionVersion] =
    stringMapping.get(vs).toRight(s"Unsupported transaction version '$vs'")

  def assertFromString(vs: String): TransactionVersion =
    data.assertRight(fromString(vs))

  def fromInt(i: Int): Either[String, TransactionVersion] =
    intMapping.get(i).toRight(s"Unsupported transaction version '$i'")

  def assertFromInt(i: Int): TransactionVersion =
    data.assertRight(fromInt(i))

  val minVersion: TransactionVersion = All.min
  def maxVersion: TransactionVersion = VDev

  // TODO(https://github.com/digital-asset/daml/issues/18240) remove these 3 feature flags and kill
  //  the transitively dead code. Make sure canton doesn't mention them anymore.
  private[lf] val minExceptions = V14
  private[lf] val minByKey = V14
  private[lf] val minInterfaces = V15

  private[lf] val minUpgrade = V31
  private[lf] val minSharedKeys = V31
  private[lf] val minExplicitDisclosure = VDev
  private[lf] val minChoiceAuthorizers = VDev

  private[lf] val assignNodeVersion: LanguageVersion => TransactionVersion = {
    import LanguageVersion._
    Map(
      // TODO(https://github.com/digital-asset/daml/issues/18240): v1 versions are only mentioned
      //  here to ensure the map is total, which is tested elsewhere. But these versions are never
      //  used by the daml3 engine. Once we delete the V1 LanguageMajorVersion, they will go away.
      v1_6 -> V14,
      v1_7 -> V14,
      v1_8 -> V14,
      v1_11 -> V14,
      v1_12 -> V14,
      v1_13 -> V14,
      v1_14 -> V14,
      v1_15 -> V15,
      v1_dev -> VDev,
      v2_1 -> V31,
      v2_dev -> VDev,
    )
  }

  private[lf] def txVersion(tx: Transaction) = {
    import scala.Ordering.Implicits.infixOrderingOps
    tx.nodes.valuesIterator.foldLeft(TransactionVersion.minVersion) {
      case (acc, action: Node.Action) => acc max action.version
      case (acc, _: Node.Rollback) => acc max minExceptions
    }
  }

  private[lf] def asVersionedTransaction(
      tx: Transaction
  ): VersionedTransaction =
    VersionedTransaction(txVersion(tx), tx.nodes, tx.roots)

  val StableVersions: VersionRange[TransactionVersion] =
    LanguageVersion.StableVersions(LanguageVersion.default.major).map(assignNodeVersion)

  private[lf] val EarlyAccessVersions: VersionRange[TransactionVersion] =
    LanguageVersion.EarlyAccessVersions(LanguageVersion.default.major).map(assignNodeVersion)

  // TODO(#17366): parameterize by major language version once there's a transaction v2
  private[lf] val DevVersions: VersionRange[TransactionVersion] =
    LanguageVersion.AllVersions(LanguageMajorVersion.V2).map(assignNodeVersion)

}
