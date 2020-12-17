// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.ImmArray
import com.daml.lf.language.LanguageVersion
import com.daml.lf.value.{Value, ValueVersion}

import scala.collection.immutable.HashMap

sealed abstract class TransactionVersion private (val protoValue: String, private val index: Int)
    extends Product
    with Serializable

/**
  * Currently supported versions of the DAML-LF transaction specification.
  */
object TransactionVersion {

  case object V10 extends TransactionVersion("10", 10)
  case object VDev extends TransactionVersion("dev", Int.MaxValue)

  val Values = List(V10, VDev)

  private[lf] implicit val Ordering: scala.Ordering[TransactionVersion] = scala.Ordering.by(_.index)

  private[this] val stringMapping = Values.iterator.map(v => v.protoValue -> v).toMap

  def fromString(vs: String): Either[String, TransactionVersion] =
    stringMapping.get(vs).toRight(s"Unsupported transaction version $vs")

  def assertFromString(vs: String): TransactionVersion =
    data.assertRight(fromString(vs))

  val minVersion = Values.min
  private[transaction] val minChoiceObservers = VDev
  private[transaction] val minNodeVersion = VDev

  private[lf] val assignNodeVersion: LanguageVersion => TransactionVersion = {
    import LanguageVersion._
    Map(
      v1_6 -> V10,
      v1_7 -> V10,
      v1_8 -> V10,
      v1_dev -> VDev,
    )
  }

  private[lf] val assignValueVersion: TransactionVersion => ValueVersion = {
    Map(
      V10 -> ValueVersion("6"),
      VDev -> ValueVersion("dev"),
    )
  }

  private[lf] def asVersionedTransaction(
      roots: ImmArray[NodeId],
      nodes: HashMap[NodeId, Node.GenNode[NodeId, Value.ContractId]],
  ): VersionedTransaction[NodeId, Value.ContractId] = {
    import scala.Ordering.Implicits.infixOrderingOps

    val txVersion = roots.iterator.foldLeft(TransactionVersion.minVersion)((acc, nodeId) =>
      acc max nodes(nodeId).version)

    VersionedTransaction(txVersion, nodes, roots)
  }

  private[lf] val StableVersions: VersionRange[TransactionVersion] =
    LanguageVersion.StableVersions.map(assignNodeVersion)

  private[lf] val DevVersions: VersionRange[TransactionVersion] =
    LanguageVersion.DevVersions.map(assignNodeVersion)

}
