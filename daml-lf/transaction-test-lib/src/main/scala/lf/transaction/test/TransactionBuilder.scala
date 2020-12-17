// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction
package test

import com.daml.lf.data.{BackStack, FrontStack, FrontStackCons, ImmArray, Ref}
import com.daml.lf.transaction.{Transaction => Tx}
import com.daml.lf.value.Value.{ContractId, ContractInst, VersionedValue}
import com.daml.lf.value.{Value => LfValue}

import scala.Ordering.Implicits.infixOrderingOps
import scala.annotation.tailrec
import scala.collection.immutable.HashMap

final class TransactionBuilder(pkgTxVersion: Ref.PackageId => TransactionVersion = _ =>
  TransactionVersion.minVersion) {

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
    val nodeId = ids.next()
    nodes += (nodeId -> node)
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
    import TransactionVersion.Ordering
    val finalNodes = nodes.transform {
      case (nid, exe: TxExercise) =>
        exe.copy(children = children(nid).toImmArray)
      case (_, node: Node.LeafOnlyNode[ContractId]) =>
        node
    }
    val finalRoots = roots.toImmArray
    val txVersion = finalRoots.iterator.foldLeft(TransactionVersion.minVersion)((acc, nodeId) =>
      acc max finalNodes(nodeId).version)
    VersionedTransaction(txVersion, finalNodes, finalRoots)
  }

  def buildSubmitted(): SubmittedTransaction = SubmittedTransaction(build())

  def buildCommitted(): CommittedTransaction = CommittedTransaction(build())

  def newCid: ContractId = ContractId.V1(newHash())

  def versionContract(contract: ContractInst[Value]): ContractInst[TxValue] =
    ContractInst.map1[Value, TxValue](transactionValue(contract.template))(contract)

  private[this] def transactionValue(templateId: Ref.TypeConName): Value => TxValue =
    value.Value.VersionedValue(pkgTxVersion(templateId.packageId), _)

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
      choiceObservers: Set[String] = Set.empty,
      byKey: Boolean = true,
  ): Exercise =
    Exercise(
      choiceObservers = choiceObservers.map(Ref.Party.assertFromString),
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
  type Node = Node.GenNode[NodeId, ContractId]
  type TxNode = Node.GenNode[NodeId, ContractId]

  type Create = Node.NodeCreate[ContractId]
  type Exercise = Node.NodeExercises[NodeId, ContractId]
  type Fetch = Node.NodeFetch[ContractId]
  type LookupByKey = Node.NodeLookupByKey[ContractId]
  type KeyWithMaintainers = Node.KeyWithMaintainers[Value]

  type TxExercise = Node.NodeExercises[NodeId, ContractId]
  type TxKeyWithMaintainers = Node.KeyWithMaintainers[TxValue]

  private val Create = Node.NodeCreate
  private val Exercise = Node.NodeExercises
  private val Fetch = Node.NodeFetch
  private val LookupByKey = Node.NodeLookupByKey
  private val KeyWithMaintainers = Node.KeyWithMaintainers

  def apply(): TransactionBuilder =
    TransactionBuilder(TransactionVersion.StableVersions.min)

  def apply(txVersion: TransactionVersion): TransactionBuilder =
    new TransactionBuilder(_ => txVersion)

  def apply(pkgLangVersion: Ref.PackageId => language.LanguageVersion): TransactionBuilder = {
    new TransactionBuilder(pkgId => TransactionVersion.assignNodeVersion(pkgLangVersion(pkgId)))
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
      TransactionVersion.minNodeVersion,
      HashMap.empty,
      ImmArray.empty,
    )
  val EmptySubmitted: SubmittedTransaction = SubmittedTransaction(Empty)
  val EmptyCommitted: CommittedTransaction = CommittedTransaction(Empty)

  def assignVersion[Cid](
      v0: Value,
      supportedVersions: VersionRange[TransactionVersion] = TransactionVersion.StableVersions
  ): Either[String, TransactionVersion] = {
    @tailrec
    def go(
        currentVersion: TransactionVersion,
        values0: FrontStack[Value],
    ): Either[String, TransactionVersion] = {
      import LfValue._
      if (currentVersion >= supportedVersions.max) {
        Right(currentVersion)
      } else {
        values0 match {
          case FrontStack() => Right(currentVersion)
          case FrontStackCons(value, values) =>
            value match {
              // for things supported since version 1, we do not need to check
              case ValueRecord(_, fs) => go(currentVersion, fs.map(v => v._2) ++: values)
              case ValueVariant(_, _, arg) => go(currentVersion, arg +: values)
              case ValueList(vs) => go(currentVersion, vs.toImmArray ++: values)
              case ValueContractId(_) | ValueInt64(_) | ValueText(_) | ValueTimestamp(_) |
                  ValueParty(_) | ValueBool(_) | ValueDate(_) | ValueUnit | ValueNumeric(_) =>
                go(currentVersion, values)
              case ValueOptional(x) =>
                go(currentVersion, x.fold(values)(_ +: values))
              case ValueTextMap(map) =>
                go(currentVersion, map.values ++: values)
              case ValueEnum(_, _) =>
                go(currentVersion, values)
              // for things added after version 10, we raise the minimum if present
              case ValueGenMap(entries) =>
                val newValues = entries.iterator.foldLeft(values) {
                  case (acc, (key, value)) => key +: value +: acc
                }
                go(currentVersion max TransactionVersion.minGenMap, newValues)
            }
        }
      }
    }

    go(supportedVersions.min, FrontStack(v0)) match {
      case Right(inferredVersion) if supportedVersions.max < inferredVersion =>
        Left(s"inferred version $inferredVersion is not supported")
      case res =>
        res
    }

  }
  @throws[IllegalArgumentException]
  def assertAssignVersion(
      v0: Value,
      supportedVersions: VersionRange[TransactionVersion] = TransactionVersion.DevVersions,
  ): TransactionVersion =
    data.assertRight(assignVersion(v0, supportedVersions))

  def asVersionedValue(
      value: Value,
      supportedVersions: VersionRange[TransactionVersion] = TransactionVersion.DevVersions,
  ): Either[String, TxValue] =
    assignVersion(value, supportedVersions).map(VersionedValue(_, value))

  @throws[IllegalArgumentException]
  def assertAsVersionedValue(
      value: Value,
      supportedVersions: VersionRange[TransactionVersion] = TransactionVersion.DevVersions,
  ): TxValue =
    data.assertRight(asVersionedValue(value, supportedVersions))

}
