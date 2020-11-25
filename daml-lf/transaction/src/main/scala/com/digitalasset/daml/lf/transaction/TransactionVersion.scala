// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.LanguageVersion
import com.daml.lf.value.Value.{ContractId, VersionedValue}
import com.daml.lf.value.{Value, ValueVersion, ValueVersions}

import scala.collection.immutable.HashMap

final case class TransactionVersion(protoValue: String)

/**
  * Currently supported versions of the DAML-LF transaction specification.
  */
private[lf] object TransactionVersions
    extends LfVersions(versionsAscending = VersionTimeline.ascendingVersions[TransactionVersion])(
      _.protoValue,
    ) {

  import VersionTimeline._
  import VersionTimeline.Implicits._

  private[transaction] val minVersion = TransactionVersion("10")
  private[transaction] val minChoiceObservers = TransactionVersion("dev")
  private[transaction] val minNodeVersion = TransactionVersion("dev")

  // Older versions are deprecated https://github.com/digital-asset/daml/issues/5220
  val StableOutputVersions: VersionRange[TransactionVersion] =
    VersionRange(TransactionVersion("10"), TransactionVersion("10"))

  val DevOutputVersions: VersionRange[TransactionVersion] =
    StableOutputVersions.copy(max = acceptedVersions.last)

  val Empty: VersionRange[TransactionVersion] =
    VersionRange(acceptedVersions.last, acceptedVersions.head)

  private[lf] def assignValueVersion(nodeVersion: TransactionVersion): ValueVersion =
    latestWhenAllPresent(ValueVersions.acceptedVersions.head, nodeVersion)

  private[lf] def assignNodeVersion(langVersion: LanguageVersion): TransactionVersion =
    VersionTimeline.latestWhenAllPresent(TransactionVersions.minVersion, langVersion)

  type UnversionedNode = Node.GenNode[NodeId, Value.ContractId, Value[Value.ContractId]]
  type VersionedNode = Node.GenNode[NodeId, Value.ContractId, VersionedValue[Value.ContractId]]

  def asVersionedTransaction(
      pkgLangVersions: Ref.PackageId => LanguageVersion,
      roots: ImmArray[NodeId],
      nodes: HashMap[NodeId, UnversionedNode],
  ): VersionedTransaction[NodeId, Value.ContractId] = {

    val versionedNodes = nodes.transform { (_, node) =>
      val nodeVersion = assignNodeVersion(pkgLangVersions(node.templateId.packageId))
      val valueVersion = assignValueVersion(nodeVersion)
      Node.VersionedNode(
        nodeVersion,
        Node.GenNode.map3(
          identity[NodeId],
          identity[ContractId],
          VersionedValue[ContractId](valueVersion, _))(node),
      )
    }

    val txVersion = roots.iterator.foldLeft(TransactionVersions.minVersion)((acc, nodeId) =>
      VersionTimeline.maxVersion(acc, versionedNodes(nodeId).version))

    VersionedTransaction(txVersion, versionedNodes, roots)
  }

}
