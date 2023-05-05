// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.language.LanguageVersion

sealed abstract class TransactionVersion private (val protoValue: String, private val index: Int)
    extends Product
    with Serializable

/** Currently supported versions of the Daml-LF transaction specification.
  */
object TransactionVersion {

  case object V10 extends TransactionVersion("10", 10)
  case object V11 extends TransactionVersion("11", 11)
  case object V12 extends TransactionVersion("12", 12)
  case object V13 extends TransactionVersion("13", 13)
  case object V14 extends TransactionVersion("14", 14)
  case object V15 extends TransactionVersion("15", 15)
  case object VDev extends TransactionVersion("dev", Int.MaxValue)

  val All = List(V10, V11, V12, V13, V14, V15, VDev)

  implicit val Ordering: scala.Ordering[TransactionVersion] =
    scala.Ordering.by(_.index)

  private[this] val stringMapping = All.iterator.map(v => v.protoValue -> v).toMap

  def fromString(vs: String): Either[String, TransactionVersion] =
    stringMapping.get(vs) match {
      case Some(value) => Right(value)
      case None =>
        Left(s"Unsupported transaction version '$vs'")
    }

  def assertFromString(vs: String): TransactionVersion =
    data.assertRight(fromString(vs))

  val minVersion: TransactionVersion = All.min
  def maxVersion: TransactionVersion = VDev

  private[lf] val minGenMap = V11
  private[lf] val minChoiceObservers = V11
  private[lf] val minNodeVersion = V11
  private[lf] val minNoVersionValue = V12
  private[lf] val minTypeErasure = V12
  // nothing was added in V13, so there are no vals: "minSomething = V13"
  private[lf] val minExceptions = V14
  private[lf] val minByKey = V14
  private[lf] val minInterfaces = V15
  private[lf] val minExplicitDisclosure = VDev
  private[lf] val minChoiceAuthorizers = VDev

  private[lf] val assignNodeVersion: LanguageVersion => TransactionVersion = {
    import LanguageVersion._
    Map(
      v1_6 -> V10,
      v1_7 -> V10,
      v1_8 -> V10,
      v1_11 -> V11,
      v1_12 -> V12,
      v1_13 -> V13,
      v1_14 -> V14,
      v1_15 -> V15,
      v1_dev -> VDev,
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
    LanguageVersion.StableVersions.map(assignNodeVersion)

  private[lf] val EarlyAccessVersions: VersionRange[TransactionVersion] =
    LanguageVersion.EarlyAccessVersions.map(assignNodeVersion)

  private[lf] val DevVersions: VersionRange[TransactionVersion] =
    LanguageVersion.DevVersions.map(assignNodeVersion)

}
