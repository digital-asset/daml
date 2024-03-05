// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.language.LanguageVersion

sealed abstract class TransactionVersion private (
    val protoValue: String,
    private[transaction] val index: Int,
) extends Product
    with Serializable

/** Currently supported versions of the Daml-LF transaction specification.
  */
object TransactionVersion {

  case object V31 extends TransactionVersion("2.1", 1)
  case object VDev extends TransactionVersion("dev", Int.MaxValue)

  val All = List(V31, VDev)

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

  // TODO(https://github.com/digital-asset/daml/issues/18240) move this feature flag to VDev.
  private[lf] val minContractKeys = V31

  private[lf] val minChoiceAuthorizers = VDev

  private[lf] val assignNodeVersion: LanguageVersion => TransactionVersion = {
    import LanguageVersion._
    Map(
      v2_1 -> V31,
      v2_dev -> VDev,
    )
  }

  private[lf] def txVersion(tx: Transaction) = {
    import scala.Ordering.Implicits.infixOrderingOps
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
    LanguageVersion.StableVersions(LanguageVersion.default.major).map(assignNodeVersion)

  private[lf] val EarlyAccessVersions: VersionRange[TransactionVersion] =
    LanguageVersion.EarlyAccessVersions(LanguageVersion.default.major).map(assignNodeVersion)

  private[lf] val DevVersions: VersionRange[TransactionVersion] =
    LanguageVersion.AllVersions(LanguageVersion.default.major).map(assignNodeVersion)

}
