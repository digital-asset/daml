// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref._
import com.daml.lf.data.ImmArray
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value._

/** Generic transaction node type for both update transactions and the
  * transaction graph.
  */
sealed abstract class Node extends Product with Serializable with CidContainer[Node] {

  def optVersion: Option[TransactionVersion] = this match {
    case node: Node.Action => Some(node.version)
    case _: Node.Rollback => None
    case _: Node.Authority => None
  }

  def mapNodeId(f: NodeId => NodeId): Node
}

object Node {

  /** action nodes parametrized over identifier type */
  sealed abstract class Action extends Node with ActionNodeInfo with CidContainer[Action] {

    def version: TransactionVersion

    private[lf] def updateVersion(version: TransactionVersion): Node

    def templateId: TypeConName

    /** The package ids used by this action node.
      */
    def packageIds: Iterable[PackageId]

    def keyOpt: Option[GlobalKeyWithMaintainers]

    final def gkeyOpt: Option[GlobalKey] = keyOpt.map(_.globalKey)

    final override protected def self: this.type = this

    /** Required authorizers (see ledger model); UNSAFE TO USE on fetch nodes of transaction with versions < 5
      *
      * The ledger model defines the fetch node actors as the nodes' required authorizers.
      * However, the our transaction data structure did not include the actors in versions < 5.
      * The usage of this method must thus be restricted to:
      * 1. settings where no fetch nodes appear (for example, the `validate` method of DAMLe, which uses it on root
      *    nodes, which are guaranteed never to contain a fetch node)
      * 2. Daml ledger implementations that do not store or process any transactions with version < 5
      */
    def requiredAuthorizers: Set[Party]

    def byKey: Boolean

    protected def versioned[X](x: X): Versioned[X] = Versioned(version, x)
  }

  /** A transaction node with no children */
  sealed trait LeafOnlyAction extends Action {
    override def mapNodeId(f: NodeId => NodeId): Node = this
  }

  /** Denotes the creation of a contract instance. */
  final case class Create(
      coid: ContractId,
      override val templateId: TypeConName,
      arg: Value,
      agreementText: String,
      signatories: Set[Party],
      stakeholders: Set[Party],
      keyOpt: Option[GlobalKeyWithMaintainers],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyAction
      with ActionNodeInfo.Create {

    @deprecated("use keyOpt", since = "2.6.0")
    def key: Option[GlobalKeyWithMaintainers] = keyOpt

    override def byKey: Boolean = false

    override private[lf] def updateVersion(version: TransactionVersion): Node.Create =
      copy(version = version)

    override def mapCid(f: ContractId => ContractId): Node.Create =
      copy(coid = f(coid), arg = arg.mapCid(f))

    override def packageIds: Iterable[PackageId] = Iterable(templateId.packageId)

    def versionedArg: Value.VersionedValue = versioned(arg)

    def coinst: Value.ContractInstance =
      Value.ContractInstance(templateId, arg)

    def versionedCoinst: Value.VersionedContractInstance = versioned(coinst)

    def versionedKey: Option[Versioned[GlobalKeyWithMaintainers]] = keyOpt.map(versioned(_))
  }

  /** Denotes that the contract identifier `coid` needs to be active for the transaction to be valid. */
  final case class Fetch(
      coid: ContractId,
      override val templateId: TypeConName,
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      override val keyOpt: Option[GlobalKeyWithMaintainers],
      override val byKey: Boolean,
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyAction
      with ActionNodeInfo.Fetch {

    @deprecated("use keyOpt", since = "2.6.0")
    def key: Option[GlobalKeyWithMaintainers] = keyOpt

    override private[lf] def updateVersion(version: TransactionVersion): Node.Fetch =
      copy(version = version)

    override def mapCid(f: ContractId => ContractId): Node.Fetch =
      copy(coid = f(coid))

    override def packageIds: Iterable[PackageId] = Iterable(templateId.packageId)
  }

  /** Denotes a transaction node for an exercise.
    * We remember the `children` of this `NodeExercises`
    * to allow segregating the graph afterwards into party-specific
    * ledgers.
    */
  final case class Exercise(
      targetCoid: ContractId,
      override val templateId: TypeConName,
      interfaceId: Option[TypeConName],
      choiceId: ChoiceName,
      consuming: Boolean,
      actingParties: Set[Party],
      chosenValue: Value,
      stakeholders: Set[Party],
      signatories: Set[Party],
      choiceObservers: Set[Party],
      children: ImmArray[NodeId],
      exerciseResult: Option[Value],
      keyOpt: Option[GlobalKeyWithMaintainers],
      override val byKey: Boolean,
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends Action
      with ActionNodeInfo.Exercise {

    def qualifiedChoiceName = QualifiedChoiceName(interfaceId, choiceId)

    @deprecated("use keyOpt", since = "2.6.0")
    def key: Option[GlobalKeyWithMaintainers] = keyOpt

    override private[lf] def updateVersion(
        version: TransactionVersion
    ): Node.Exercise =
      copy(version = version)

    override def mapCid(f: ContractId => ContractId): Node.Exercise = copy(
      targetCoid = f(targetCoid),
      chosenValue = chosenValue.mapCid(f),
      exerciseResult = exerciseResult.map(_.mapCid(f)),
    )

    override def mapNodeId(f: NodeId => NodeId): Node.Exercise =
      copy(children = children.map(f))

    override def packageIds: Iterable[PackageId] =
      Iterable(templateId.packageId) ++ interfaceId.map(_.packageId)

    def versionedChosenValue: Value.VersionedValue = versioned(chosenValue)

    def versionedExerciseResult: Option[Value.VersionedValue] = exerciseResult.map(versioned)

    def versionedKey: Option[Versioned[GlobalKeyWithMaintainers]] = keyOpt.map(versioned)

  }

  final case class LookupByKey(
      override val templateId: TypeConName,
      key: GlobalKeyWithMaintainers,
      result: Option[ContractId],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyAction
      with ActionNodeInfo.LookupByKey {

    override def keyOpt: Some[GlobalKeyWithMaintainers] = Some(key)

    def gkey: GlobalKey = key.globalKey

    override def mapCid(f: ContractId => ContractId): Node.LookupByKey =
      copy(result = result.map(f))

    override def keyMaintainers: Set[Party] = key.maintainers
    override def hasResult: Boolean = result.isDefined
    override def byKey: Boolean = true

    override private[lf] def updateVersion(version: TransactionVersion): Node.LookupByKey =
      copy(version = version)

    override def packageIds: Iterable[PackageId] = Iterable(templateId.packageId)
  }

  @deprecated("use GlobalKey", since = "2.6.0")
  type KeyWithMaintainers = GlobalKey
  @deprecated("use GlobalKey", since = "2.6.0")
  val KeyWithMaintainers = GlobalKey

  @deprecated("use VersionedGlobalKey", since = "2.6.0")
  type VersionedKeyWithMaintainers = VersionedGlobalKey

  final case class Rollback(
      children: ImmArray[NodeId]
  ) extends Node {

    override def mapCid(f: ContractId => ContractId): Node.Rollback = this
    override def mapNodeId(f: NodeId => NodeId): Node.Rollback =
      copy(children.map(f))

    override protected def self: Node = this
  }

  final case class Authority(
      obtained: Set[Party],
      children: ImmArray[NodeId],
  ) extends Node {

    override def mapCid(f: ContractId => ContractId): Node.Authority = this
    override def mapNodeId(f: NodeId => NodeId): Node.Authority =
      copy(children = children.map(f))

    override protected def self: Node = this
  }

}

final case class NodeId(index: Int)
