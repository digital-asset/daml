// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.ImmArray
import com.daml.lf.language.LanguageVersion
import com.daml.lf.value.Value

import scala.collection.immutable.HashMap

sealed abstract class TransactionVersion private (val protoValue: String, private val index: Int)
    extends Product
    with Serializable

/** Currently supported versions of the DAML-LF transaction specification.
  */
object TransactionVersion {

  case object V10 extends TransactionVersion("10", 10)
  case object V11 extends TransactionVersion("11", 11)
  case object VDev extends TransactionVersion("dev", Int.MaxValue)

  val All = List(V10, V11, VDev)

  private[lf] implicit val Ordering: scala.Ordering[TransactionVersion] = scala.Ordering.by(_.index)

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
  private[lf] val minTypeErasure = VDev

  private[lf] val assignNodeVersion: LanguageVersion => TransactionVersion = {
    import LanguageVersion._
    Map(
      v1_6 -> V10,
      v1_7 -> V10,
      v1_8 -> V10,
      v1_11 -> V11,
      v1_dev -> VDev,
    )
  }

  private[lf] def asVersionedTransaction(
      roots: ImmArray[NodeId],
      nodes: HashMap[NodeId, Node.GenNode[NodeId, Value.ContractId]],
  ): VersionedTransaction[NodeId, Value.ContractId] = {
    import scala.Ordering.Implicits.infixOrderingOps

    val txVersion = roots.iterator.foldLeft(TransactionVersion.minVersion)((acc, nodeId) =>
      acc max nodes(nodeId).version
    )

    VersionedTransaction(txVersion, nodes, roots)
  }

  private[lf] val StableVersions: VersionRange[TransactionVersion] =
    LanguageVersion.StableVersions.map(assignNodeVersion)

  private[lf] val DevVersions: VersionRange[TransactionVersion] =
    LanguageVersion.DevVersions.map(assignNodeVersion)

}
