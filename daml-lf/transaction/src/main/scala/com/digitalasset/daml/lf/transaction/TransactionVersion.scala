// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.LanguageVersion
import com.daml.lf.value.Value.VersionedValue
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

  private[transaction] val minVersion = TransactionVersion("1")
  private[transaction] val minKeyOrLookupByKey = TransactionVersion("3")
  private[transaction] val minFetchActors = TransactionVersion("5")
  private[transaction] val minNoControllers = TransactionVersion("6")
  private[transaction] val minExerciseResult = TransactionVersion("7")
  private[transaction] val minContractKeyInExercise = TransactionVersion("8")
  private[transaction] val minMaintainersInExercise = TransactionVersion("9")
  private[transaction] val minContractKeyInFetch = TransactionVersion("10")
  private[transaction] val minChoiceObservers = TransactionVersion("dev")

  // Older versions are deprecated https://github.com/digital-asset/daml/issues/5220
  val StableOutputVersions: VersionRange[TransactionVersion] =
    VersionRange(TransactionVersion("10"), TransactionVersion("10"))

  val DevOutputVersions: VersionRange[TransactionVersion] =
    StableOutputVersions.copy(max = acceptedVersions.last)

  val Empty: VersionRange[TransactionVersion] =
    VersionRange(acceptedVersions.last, acceptedVersions.head)

  private[lf] def assignValueVersion(transactionVersion: TransactionVersion): ValueVersion =
    latestWhenAllPresent(
      ValueVersions.acceptedVersions.head,
      transactionVersion,
    )

  private[lf] def assignVersions(
      as: Seq[SpecifiedVersion],
  ): TransactionVersion =
    VersionTimeline.latestWhenAllPresent(
      minVersion,
      (DevOutputVersions.min: SpecifiedVersion) +: as: _*,
    )

  type UnversionedNode = Node.GenNode[NodeId, Value.ContractId, Value[Value.ContractId]]
  type VersionedNode = Node.GenNode[NodeId, Value.ContractId, VersionedValue[Value.ContractId]]

  def asVersionedTransaction(
      pkgLangVersions: Ref.PackageId => LanguageVersion,
      roots: ImmArray[NodeId],
      nodes: HashMap[NodeId, UnversionedNode],
  ): VersionedTransaction[NodeId, Value.ContractId] = {

    import VersionTimeline.Implicits._

    val langVersions: Iterator[SpecifiedVersion] =
      roots.iterator.map(nid => pkgLangVersions(nodes(nid).templateId.packageId))

    val txVersion = assignVersions(langVersions.toList)
    val versionNode: UnversionedNode => VersionedNode =
      Node.GenNode.map3(identity, identity, VersionedValue(assignValueVersion(txVersion), _))
    VersionedTransaction(
      assignVersions(langVersions.toList),
      GenTransaction(nodes = nodes.transform((_, n) => versionNode(n)), roots = roots)
    )
  }

}
