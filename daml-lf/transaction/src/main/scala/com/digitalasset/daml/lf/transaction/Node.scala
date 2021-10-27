// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, ScalazEqual}
import com.daml.lf.value.Value.{ContractId, VersionedValue}
import com.daml.lf.value._
import scalaz.Equal
import scalaz.syntax.equal._

/** Generic transaction node type for both update transactions and the
  * transaction graph.
  */
object Node {

  sealed abstract class GenNode extends Product with Serializable with CidContainer[GenNode] {

    def optVersion: Option[TransactionVersion] = this match {
      case node: GenActionNode => Some(node.version)
      case _: NodeRollback => None
    }

    def mapNodeId(f: NodeId => NodeId): GenNode
  }

  /** action nodes parametrized over identifier type */
  sealed abstract class GenActionNode
      extends GenNode
      with ActionNodeInfo
      with CidContainer[GenActionNode] {

    def version: TransactionVersion

    private[lf] def updateVersion(version: TransactionVersion): GenNode

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

    protected def versionValue[Cid2 >: ContractId](v: Value): VersionedValue =
      VersionedValue(version, v)
  }

  /** A transaction node with no children */
  sealed trait LeafOnlyActionNode extends GenActionNode {
    override def mapNodeId(f: NodeId => NodeId): GenNode = this
  }

  /** Denotes the creation of a contract instance. */
  final case class NodeCreate(
      coid: ContractId,
      override val templateId: TypeConName,
      arg: Value,
      agreementText: String,
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[KeyWithMaintainers[Value]],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyActionNode
      with ActionNodeInfo.Create {

    override def byKey: Boolean = false

    override private[lf] def updateVersion(version: TransactionVersion): NodeCreate =
      copy(version = version)

    override def mapCid(f: ContractId => ContractId): NodeCreate =
      copy(coid = f(coid), arg = arg.mapCid(f), key = key.map(_.mapCid(f)))

    def versionedArg: VersionedValue = versionValue(arg)

    def coinst: Value.ContractInstance =
      Value.ContractInstance(templateId, arg, agreementText)

    def versionedCoinst: Value.VersionedContractInstance =
      Value.VersionedContractInstance(version, templateId, arg, agreementText)

    def versionedKey: Option[KeyWithMaintainers[Value.VersionedValue]] =
      key.map(_.map(versionValue))
  }

  /** Denotes that the contract identifier `coid` needs to be active for the transaction to be valid. */
  final case class NodeFetch(
      coid: ContractId,
      override val templateId: TypeConName,
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[KeyWithMaintainers[Value]],
      override val byKey: Boolean, // invariant (!byKey || exerciseResult.isDefined)
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyActionNode
      with ActionNodeInfo.Fetch {

    override private[lf] def updateVersion(version: TransactionVersion): NodeFetch =
      copy(version = version)

    final override def mapCid(f: ContractId => ContractId): NodeFetch =
      copy(coid = f(coid), key = key.map(_.mapCid(f)))

    def versionedKey: Option[KeyWithMaintainers[Value.VersionedValue]] =
      key.map(_.map(versionValue))
  }

  /** Denotes a transaction node for an exercise.
    * We remember the `children` of this `NodeExercises`
    * to allow segregating the graph afterwards into party-specific
    * ledgers.
    */
  final case class NodeExercises(
      targetCoid: ContractId,
      override val templateId: TypeConName,
      choiceId: ChoiceName,
      consuming: Boolean,
      actingParties: Set[Party],
      chosenValue: Value,
      stakeholders: Set[Party],
      signatories: Set[Party],
      choiceObservers: Set[Party],
      children: ImmArray[NodeId],
      exerciseResult: Option[Value],
      key: Option[KeyWithMaintainers[Value]],
      override val byKey: Boolean, // invariant (!byKey || exerciseResult.isDefined)
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends GenActionNode
      with ActionNodeInfo.Exercise {

    override private[lf] def updateVersion(
        version: TransactionVersion
    ): NodeExercises =
      copy(version = version)

    final override def mapCid(f: ContractId => ContractId): NodeExercises = copy(
      targetCoid = f(targetCoid),
      chosenValue = chosenValue.mapCid(f),
      exerciseResult = exerciseResult.map(_.mapCid(f)),
      key = key.map(_.mapCid(f)),
    )

    override def mapNodeId(f: NodeId => NodeId): NodeExercises =
      copy(children = children.map(f))

    def versionedChosenValue: Value.VersionedValue =
      versionValue(chosenValue)

    def versionedExerciseResult: Option[Value.VersionedValue] =
      exerciseResult.map(versionValue)

    def versionedKey: Option[KeyWithMaintainers[Value.VersionedValue]] =
      key.map(_.map(versionValue))
  }

  final case class NodeLookupByKey(
      override val templateId: TypeConName,
      key: KeyWithMaintainers[Value],
      result: Option[ContractId],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyActionNode
      with ActionNodeInfo.LookupByKey {

    final override def mapCid(f: ContractId => ContractId): NodeLookupByKey =
      copy(key = key.mapCid(f), result = result.map(f))

    override def keyMaintainers: Set[Party] = key.maintainers
    override def hasResult: Boolean = result.isDefined
    override def byKey: Boolean = true

    override private[lf] def updateVersion(version: TransactionVersion): NodeLookupByKey =
      copy(version = version)

    def versionedKey: KeyWithMaintainers[Value.VersionedValue] =
      key.map(versionValue)
  }

  final case class KeyWithMaintainers[+Val](key: Val, maintainers: Set[Party]) {

    def map[Val2](f: Val => Val2): KeyWithMaintainers[Val2] =
      copy(key = f(key))
  }

  object KeyWithMaintainers {
    implicit class CidContainerInstance[Val <: CidContainer[Val]](key: KeyWithMaintainers[Val])
        extends CidContainer[KeyWithMaintainers[Val]] {
      override def self = key
      final override def mapCid(f: ContractId => ContractId): KeyWithMaintainers[Val] =
        self.copy(key = self.key.mapCid(f))
    }

    implicit def equalInstance[Val: Equal]: Equal[KeyWithMaintainers[Val]] =
      ScalazEqual.withNatural(Equal[Val].equalIsNatural) { (a, b) =>
        import a._
        val KeyWithMaintainers(bKey, bMaintainers) = b
        key === bKey && maintainers == bMaintainers
      }
  }

  final case class NodeRollback(
      children: ImmArray[NodeId]
  ) extends GenNode {

    final override def mapCid(f: ContractId => ContractId): NodeRollback = this
    override def mapNodeId(f: NodeId => NodeId): NodeRollback =
      copy(children.map(f))

    override protected def self: GenNode = this
  }

}

final case class NodeId(index: Int)
