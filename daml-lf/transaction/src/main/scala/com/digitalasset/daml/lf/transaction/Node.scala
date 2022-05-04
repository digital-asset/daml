// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  }

  def mapNodeId(f: NodeId => NodeId): Node
}

object Node {

  @deprecated("use Node", since = "1.18.0")
  type GenNode = Node

  @deprecated("use Node.Action", since = "1.18.0")
  type GenAction = Action

  /** action nodes parametrized over identifier type */
  sealed abstract class Action extends Node with ActionNodeInfo with CidContainer[Action] {

    def version: TransactionVersion

    private[lf] def updateVersion(version: TransactionVersion): Node

    def templateId: TypeConName

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

  @deprecated("use Node.LeafOnlyAction", since = "1.18.0")
  type LeafOnlyActionNode = LeafOnlyAction

  /** A transaction node with no children */
  sealed trait LeafOnlyAction extends Action {
    override def mapNodeId(f: NodeId => NodeId): Node = this
  }

  @deprecated("use Node.Create", since = "1.18.0")
  type NodeCreate = Create
  @deprecated("use Node.Create", since = "1.18.0")
  val NodeCreate: Create.type = Create

  /** Denotes the creation of a contract instance. */
  final case class Create(
      coid: ContractId,
      override val templateId: TypeConName,
      arg: Value,
      agreementText: String,
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[KeyWithMaintainers],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyAction
      with ActionNodeInfo.Create {

    override def byKey: Boolean = false

    override private[lf] def updateVersion(version: TransactionVersion): Node.Create =
      copy(version = version)

    override def mapCid(f: ContractId => ContractId): Node.Create =
      copy(coid = f(coid), arg = arg.mapCid(f), key = key.map(_.mapCid(f)))

    def versionedArg: Value.VersionedValue = versioned(arg)

    def coinst: Value.ContractInstance =
      Value.ContractInstance(templateId, arg, agreementText)

    def versionedCoinst: Value.VersionedContractInstance = versioned(coinst)

    def versionedKey: Option[VersionedKeyWithMaintainers] = key.map(versioned)
  }

  @deprecated("use Node.Fetch", since = "1.18.0")
  type NodeFetch = Fetch
  @deprecated("use Node.Fetch", since = "1.18.0")
  val NodeFetch: Fetch.type = Fetch

  /** Denotes that the contract identifier `coid` needs to be active for the transaction to be valid. */
  final case class Fetch(
      coid: ContractId,
      override val templateId: TypeConName,
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[KeyWithMaintainers],
      override val byKey: Boolean, // invariant (!byKey || exerciseResult.isDefined)
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyAction
      with ActionNodeInfo.Fetch {

    override private[lf] def updateVersion(version: TransactionVersion): Node.Fetch =
      copy(version = version)

    override def mapCid(f: ContractId => ContractId): Node.Fetch =
      copy(coid = f(coid), key = key.map(_.mapCid(f)))

    def versionedKey: Option[VersionedKeyWithMaintainers] = key.map(versioned)
  }

  @deprecated("use Node.Exercise", since = "1.18.0")
  type NodeExercises = Exercise
  @deprecated("use Node.Exercise", since = "1.18.0")
  val NodeExercises: Exercise.type = Exercise

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
      key: Option[KeyWithMaintainers],
      override val byKey: Boolean, // invariant (!byKey || exerciseResult.isDefined)
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends Action
      with ActionNodeInfo.Exercise {

    override private[lf] def updateVersion(
        version: TransactionVersion
    ): Node.Exercise =
      copy(version = version)

    override def mapCid(f: ContractId => ContractId): Node.Exercise = copy(
      targetCoid = f(targetCoid),
      chosenValue = chosenValue.mapCid(f),
      exerciseResult = exerciseResult.map(_.mapCid(f)),
      key = key.map(_.mapCid(f)),
    )

    override def mapNodeId(f: NodeId => NodeId): Node.Exercise =
      copy(children = children.map(f))

    def versionedChosenValue: Value.VersionedValue = versioned(chosenValue)

    def versionedExerciseResult: Option[Value.VersionedValue] = exerciseResult.map(versioned)

    def versionedKey: Option[VersionedKeyWithMaintainers] = key.map(versioned)
  }

  @deprecated("use Node.LookupByKey", since = "1.18.0")
  type NodeLookupByKey = LookupByKey
  @deprecated("use Node.LookupByKey", since = "1.18.0")
  val NodeLookupByKey: LookupByKey.type = LookupByKey

  final case class LookupByKey(
      override val templateId: TypeConName,
      key: KeyWithMaintainers,
      result: Option[ContractId],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyAction
      with ActionNodeInfo.LookupByKey {

    override def mapCid(f: ContractId => ContractId): Node.LookupByKey =
      copy(key = key.mapCid(f), result = result.map(f))

    override def keyMaintainers: Set[Party] = key.maintainers
    override def hasResult: Boolean = result.isDefined
    override def byKey: Boolean = true

    override private[lf] def updateVersion(version: TransactionVersion): Node.LookupByKey =
      copy(version = version)

    def versionedKey: VersionedKeyWithMaintainers = versioned(key)
  }

  final case class KeyWithMaintainers(key: Value, maintainers: Set[Party])
      extends CidContainer[KeyWithMaintainers] {

    def map(f: Value => Value): KeyWithMaintainers =
      copy(key = f(key))

    override protected def self: this.type = this

    override def mapCid(f: ContractId => ContractId): KeyWithMaintainers =
      copy(key = key.mapCid(f))
  }

  type VersionedKeyWithMaintainers = Versioned[KeyWithMaintainers]

  @deprecated("use Node.Rollback", since = "1.18.0")
  type NodeRollback = Rollback
  @deprecated("use Node.Rollback", since = "1.18.0")
  val NodeRollback: Rollback.type = Rollback

  final case class Rollback(
      children: ImmArray[NodeId]
  ) extends Node {

    override def mapCid(f: ContractId => ContractId): Node.Rollback = this
    override def mapNodeId(f: NodeId => NodeId): Node.Rollback =
      copy(children.map(f))

    override protected def self: Node = this
  }

}

final case class NodeId(index: Int)
