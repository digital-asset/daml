// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.ImmArray
import com.daml.lf.language.LanguageVersion
import com.daml.lf.value.{Value, ValueVersion}
import scalaz.NonEmptyList

import scala.collection.immutable.HashMap

final case class TransactionVersion(protoValue: String)

/**
  * Currently supported versions of the DAML-LF transaction specification.
  */
object TransactionVersion
    extends LfVersions(versionsAscending =
      NonEmptyList(new TransactionVersion("10"), new TransactionVersion("dev")))(
      _.protoValue,
    ) {

  private[lf] implicit val Ordering: Ordering[TransactionVersion] = mkOrdering

  private[lf] val List(v10, vDev) = acceptedVersions

  val minVersion = v10
  private[transaction] val minChoiceObservers = vDev
  private[transaction] val minNodeVersion = vDev

  // Older versions are deprecated https://github.com/digital-asset/daml/issues/5220
  private[lf] val StableOutputVersions: VersionRange[TransactionVersion] =
    VersionRange(v10, v10)

  private[lf] val DevOutputVersions: VersionRange[TransactionVersion] =
    StableOutputVersions.copy(max = acceptedVersions.last)

  private[lf] val assignNodeVersion: LanguageVersion => TransactionVersion = {
    import LanguageVersion._
    Map(
      v1_6 -> v10,
      v1_7 -> v10,
      v1_8 -> v10,
      v1_dev -> vDev,
    )
  }

  private[lf] val assignValueVersion: TransactionVersion => ValueVersion = {
    Map(
      v10 -> ValueVersion("6"),
      vDev -> ValueVersion("dev"),
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

}
