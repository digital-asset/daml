// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction
package test

import com.daml.lf.data.{BackStack, ImmArray, Ref}
import com.daml.lf.transaction.Node.GenNode
import com.daml.lf.transaction.{Transaction => Tx}
import com.daml.lf.value.Value.{ContractId, ContractInst}
import com.daml.lf.value.{ValueVersions, Value => LfValue}

import scala.collection.immutable.HashMap

final class TransactionBuilder(pkgTxVersion: Ref.PackageId => TransactionVersion = _ =>
  TransactionVersions.minVersion) {

  import TransactionBuilder._

  private[this] val newHash: () => crypto.Hash = {
    val bytes = Array.ofDim[Byte](crypto.Hash.underlyingHashLength)
    scala.util.Random.nextBytes(bytes)
    crypto.Hash.secureRandom(crypto.Hash.assertFromByteArray(bytes))
  }

  private[this] val ids = Iterator.from(0).map(NodeId(_))
  private[this] var nodes = HashMap.empty[NodeId, TxNode]
  private[this] var children =
    HashMap.empty[NodeId, BackStack[NodeId]].withDefaultValue(BackStack.empty)
  private[this] var roots = BackStack.empty[NodeId]

  private[this] def newNode(node: Node): NodeId = {
    lazy val nodeId = ids.next() // lazy to avoid getting the next id if the method later throws
    nodes += (nodeId -> versionN(node))
    nodeId
  }

  def add(node: Node): NodeId = ids.synchronized {
    val nodeId = newNode(node)
    roots = roots :+ nodeId
    nodeId
  }

  def add(node: Node, parentId: NodeId): NodeId = ids.synchronized {
    lazy val nodeId = newNode(node) // lazy to avoid getting the next id if the method later throws
    nodes(parentId) match {
      case _: TxExercise =>
        children += parentId -> (children(parentId) :+ nodeId)
      case _ =>
        throw new IllegalArgumentException(
          s"Node ${parentId.index} either does not exist or is not an exercise")
    }
    nodeId
  }

  def build(): Tx.Transaction = ids.synchronized {
    import VersionTimeline.Implicits._

    val finalNodes = nodes.transform {
      case (nid, exe: TxExercise) =>
        exe.copy(children = children(nid).toImmArray)
      case (_, node: Node.LeafOnlyNode[ContractId, TxValue]) =>
        node
    }
    val finalRoots = roots.toImmArray
    val nodesVersions =
      finalRoots.iterator
        .map(n => finalNodes(n).version: VersionTimeline.SpecifiedVersion)
        .toSeq
    val txVersion =
      VersionTimeline
        .latestWhenAllPresent(TransactionVersions.StableOutputVersions.min, nodesVersions: _*)
    VersionedTransaction(txVersion, finalNodes, finalRoots)
  }

  def buildSubmitted(): SubmittedTransaction = SubmittedTransaction(build())

  def buildCommitted(): CommittedTransaction = CommittedTransaction(build())

  def newCid: ContractId = ContractId.V1(newHash())

  private[this] def pkgValVersion(pkgId: Ref.PackageId) = {
    import VersionTimeline.Implicits._
    VersionTimeline.latestWhenAllPresent(
      ValueVersions.StableOutputVersions.min,
      pkgTxVersion(pkgId))
  }

  def versionContract(contract: ContractInst[Value]): ContractInst[TxValue] =
    ContractInst.map1[Value, TxValue](versionValue(contract.template))(contract)

  private[this] def versionValue(templateId: Ref.TypeConName): Value => TxValue =
    value.Value.VersionedValue(pkgValVersion(templateId.packageId), _)

  private[this] def versionN(node: Node): TxNode =
    GenNode.map3(identity[NodeId], identity[ContractId], versionValue(node.templateId))(node)

  def create(
      id: String,
      template: String,
      argument: Value,
      signatories: Seq[String],
      observers: Seq[String],
      key: Option[String],
  ): Create = {
    val templateId = Ref.Identifier.assertFromString(template)
    Create(
      coid = ContractId.assertFromString(id),
      coinst = ContractInst(
        template = templateId,
        arg = argument,
        agreementText = "",
      ),
      optLocation = None,
      signatories = signatories.map(Ref.Party.assertFromString).toSet,
      stakeholders = signatories.union(observers).map(Ref.Party.assertFromString).toSet,
      key = key.map(keyWithMaintainers(maintainers = signatories, _)),
      version = pkgTxVersion(templateId.packageId),
    )
  }

  def exercise(
      contract: Create,
      choice: String,
      consuming: Boolean,
      actingParties: Set[String],
      argument: Value,
      byKey: Boolean = true,
  ): Exercise =
    Exercise(
      choiceObservers = Set.empty, //FIXME #7709: take observers as argument (pref no default value)
      targetCoid = contract.coid,
      templateId = contract.coinst.template,
      choiceId = Ref.ChoiceName.assertFromString(choice),
      optLocation = None,
      consuming = consuming,
      actingParties = actingParties.map(Ref.Party.assertFromString),
      chosenValue = argument,
      stakeholders = contract.stakeholders,
      signatories = contract.signatories,
      children = ImmArray.empty,
      exerciseResult = None,
      key = contract.key,
      byKey = byKey,
      version = pkgTxVersion(contract.coinst.template.packageId),
    )

  def exerciseByKey(
      contract: Create,
      choice: String,
      consuming: Boolean,
      actingParties: Set[String],
      argument: Value,
  ): Exercise =
    exercise(contract, choice, consuming, actingParties, argument, byKey = true)

  def fetch(contract: Create, byKey: Boolean = true): Fetch =
    Fetch(
      coid = contract.coid,
      templateId = contract.coinst.template,
      optLocation = None,
      actingParties = contract.signatories.map(Ref.Party.assertFromString),
      signatories = contract.signatories,
      stakeholders = contract.stakeholders,
      key = contract.key,
      byKey = byKey,
      version = pkgTxVersion(contract.coinst.template.packageId),
    )

  def fetchByKey(contract: Create): Fetch =
    fetch(contract, byKey = true)

  def lookupByKey(contract: Create, found: Boolean): LookupByKey =
    LookupByKey(
      templateId = contract.coinst.template,
      optLocation = None,
      key = contract.key.get,
      result = if (found) Some(contract.coid) else None,
      version = pkgTxVersion(contract.coinst.template.packageId),
    )

}

object TransactionBuilder {

  type Value = value.Value[ContractId]
  type TxValue = value.Value.VersionedValue[ContractId]
  type Node = Node.GenNode[NodeId, ContractId, Value]
  type TxNode = Node.GenNode.WithTxValue[NodeId, ContractId]

  type Create = Node.NodeCreate[ContractId, Value]
  type Exercise = Node.NodeExercises[NodeId, ContractId, Value]
  type Fetch = Node.NodeFetch[ContractId, Value]
  type LookupByKey = Node.NodeLookupByKey[ContractId, Value]
  type KeyWithMaintainers = Node.KeyWithMaintainers[Value]

  type TxExercise = Node.NodeExercises[NodeId, ContractId, TxValue]
  type TxKeyWithMaintainers = Node.KeyWithMaintainers[TxValue]

  private val Create = Node.NodeCreate
  private val Exercise = Node.NodeExercises
  private val Fetch = Node.NodeFetch
  private val LookupByKey = Node.NodeLookupByKey
  private val KeyWithMaintainers = Node.KeyWithMaintainers

  def apply(): TransactionBuilder =
    TransactionBuilder(TransactionVersions.StableOutputVersions.min)

  def apply(txVersion: TransactionVersion): TransactionBuilder =
    new TransactionBuilder(_ => txVersion)

  def apply(pkgLangVersion: Ref.PackageId => language.LanguageVersion): TransactionBuilder = {
    def pkgTxVersion(pkgId: Ref.PackageId) = {
      import VersionTimeline.Implicits._
      VersionTimeline.latestWhenAllPresent(
        TransactionVersions.StableOutputVersions.min,
        pkgLangVersion(pkgId)
      )
    }
    new TransactionBuilder(pkgTxVersion)
  }

  def record(fields: (String, String)*): Value =
    LfValue.ValueRecord(
      tycon = None,
      fields = ImmArray(
        fields.map {
          case (name, value) =>
            (Some(Ref.Name.assertFromString(name)), LfValue.ValueText(value))
        },
      )
    )

  def tuple(values: String*): Value =
    record(values.zipWithIndex.map { case (v, i) => s"_$i" -> v }: _*)

  def keyWithMaintainers(maintainers: Seq[String], value: String): KeyWithMaintainers =
    KeyWithMaintainers(
      key = tuple(maintainers :+ value: _*),
      maintainers = maintainers.map(Ref.Party.assertFromString).toSet,
    )

  def just(node: Node, nodes: Node*): Tx.Transaction = {
    val builder = TransactionBuilder()
    val _ = builder.add(node)
    for (node <- nodes) {
      val _ = builder.add(node)
    }
    builder.build()
  }

  def justSubmitted(node: Node, nodes: Node*): SubmittedTransaction =
    SubmittedTransaction(just(node, nodes: _*))

  def justCommitted(node: Node, nodes: Node*): CommittedTransaction =
    CommittedTransaction(just(node, nodes: _*))

  // not valid transactions.
  val Empty: Tx.Transaction =
    VersionedTransaction(
      TransactionVersions.StableOutputVersions.min,
      HashMap.empty,
      ImmArray.empty,
    )
  val EmptySubmitted: SubmittedTransaction = SubmittedTransaction(Empty)
  val EmptyCommitted: CommittedTransaction = CommittedTransaction(Empty)

}
