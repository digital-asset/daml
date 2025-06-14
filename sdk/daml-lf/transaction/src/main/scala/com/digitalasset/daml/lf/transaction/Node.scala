// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.value.Value.{ContractId, VersionedThinContractInstance}
import com.digitalasset.daml.lf.value._

/** Generic transaction node type for both update transactions and the
  * transaction graph.
  */
sealed abstract class Node extends Product with Serializable with CidContainer[Node] {

  def optVersion: Option[TransactionVersion] = this match {
    case node: Node.Action => Some(node.version)
    case _: Node.Rollback => None
  }

  def mapNodeId(f: NodeId => NodeId): Node

  /** String describing the type of node
    */
  // Used by Canton to report errors
  def nodeType: String = productPrefix
}

object Node {

  /** action nodes parametrized over identifier type */
  sealed abstract class Action extends Node with CidContainer[Action] {

    def version: TransactionVersion

    private[lf] def updateVersion(version: TransactionVersion): Node

    def packageName: PackageName

    def templateId: TypeConId

    /** The package ids used by this action node.
      */
    def packageIds: Iterable[PackageId]

    def keyOpt: Option[GlobalKeyWithMaintainers]

    final def gkeyOpt: Option[GlobalKey] = keyOpt.map(_.globalKey)

    def versionedKeyOpt: Option[Versioned[GlobalKeyWithMaintainers]] =
      keyOpt.map(Versioned(version, _))

    /** Compute the informees of a node based on the ledger model definition.
      *
      * Refer to https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#projections
      */
    def informeesOfNode: Set[Party]

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
  sealed trait LeafOnlyAction extends Action with CidContainer[LeafOnlyAction] {
    override def mapNodeId(f: NodeId => NodeId): Node = this
  }

  /** Denotes the creation of a contract instance. */
  final case class Create(
      coid: ContractId,
      override val packageName: PackageName,
      override val templateId: TypeConId,
      arg: Value,
      signatories: Set[Party],
      stakeholders: Set[Party],
      keyOpt: Option[GlobalKeyWithMaintainers],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyAction
      with CidContainer[Create] {

    @deprecated("use keyOpt", since = "2.6.0")
    def key: Option[GlobalKeyWithMaintainers] = keyOpt

    override def byKey: Boolean = false

    override private[lf] def updateVersion(version: TransactionVersion): Node.Create =
      copy(version = version, keyOpt = keyOpt.map(rehash))

    override def mapCid(f: ContractId => ContractId): Node.Create =
      copy(coid = f(coid), arg = arg.mapCid(f))

    override def packageIds: Iterable[PackageId] = Iterable(templateId.packageId)

    def versionedArg: Value.VersionedValue = versioned(arg)

    def coinst: Value.ThinContractInstance =
      Value.ThinContractInstance(packageName, templateId, arg)

    def versionedCoinst: Value.VersionedThinContractInstance = versioned(coinst)

    def versionedKey: Option[Versioned[GlobalKeyWithMaintainers]] = keyOpt.map(versioned(_))

    override def informeesOfNode: Set[Party] = stakeholders
    override def requiredAuthorizers: Set[Party] = signatories
  }

  object Create {

    def apply(
        coid: ContractId,
        contract: VersionedThinContractInstance,
        signatories: Set[Party],
        stakeholders: Set[Party],
        key: Option[GlobalKeyWithMaintainers],
    ): Create =
      Create(
        coid = coid,
        packageName = contract.unversioned.packageName,
        templateId = contract.unversioned.template,
        arg = contract.unversioned.arg,
        signatories = signatories,
        stakeholders = stakeholders,
        keyOpt = key,
        version = contract.version,
      )

  }

  /** Denotes that the contract identifier `coid` needs to be active for the transaction to be valid. */
  final case class Fetch(
      coid: ContractId,
      override val packageName: PackageName,
      override val templateId: TypeConId,
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      override val keyOpt: Option[GlobalKeyWithMaintainers],
      override val byKey: Boolean,
      val interfaceId: Option[TypeConId],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyAction
      with CidContainer[Fetch] {
    @deprecated("use keyOpt", since = "2.6.0")
    def key: Option[GlobalKeyWithMaintainers] = keyOpt

    override private[lf] def updateVersion(version: TransactionVersion): Node.Fetch =
      copy(version = version, keyOpt = keyOpt.map(rehash))

    override def mapCid(f: ContractId => ContractId): Node.Fetch =
      copy(coid = f(coid))

    override def packageIds: Iterable[PackageId] =
      Iterable(templateId.packageId) ++ interfaceId.map(_.packageId)

    override def informeesOfNode: Set[Party] = signatories | actingParties
    override def requiredAuthorizers: Set[Party] = actingParties
  }

  /** Denotes a transaction node for an exercise.
    * We remember the `children` of this `NodeExercises`
    * to allow segregating the graph afterwards into party-specific
    * ledgers.
    */
  final case class Exercise(
      targetCoid: ContractId,
      override val packageName: PackageName,
      override val templateId: TypeConId,
      interfaceId: Option[TypeConId],
      choiceId: ChoiceName,
      consuming: Boolean,
      actingParties: Set[Party],
      chosenValue: Value,
      stakeholders: Set[Party],
      signatories: Set[Party],
      choiceObservers: Set[Party],
      choiceAuthorizers: Option[Set[Party]],
      children: ImmArray[NodeId],
      exerciseResult: Option[Value],
      keyOpt: Option[GlobalKeyWithMaintainers],
      override val byKey: Boolean,
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends Action
      with CidContainer[Exercise] {

    def qualifiedChoiceName = QualifiedChoiceId(interfaceId, choiceId)

    @deprecated("use keyOpt", since = "2.6.0")
    def key: Option[GlobalKeyWithMaintainers] = keyOpt

    override private[lf] def updateVersion(version: TransactionVersion): Node.Exercise =
      copy(version = version, keyOpt = keyOpt.map(rehash))

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

    override def informeesOfNode: Set[Party] =
      if (consuming)
        stakeholders | actingParties | choiceObservers | choiceAuthorizers.getOrElse(Set.empty)
      else
        signatories | actingParties | choiceObservers | choiceAuthorizers.getOrElse(Set.empty)
    override def requiredAuthorizers: Set[Party] = actingParties
  }

  final case class LookupByKey(
      override val packageName: PackageName,
      override val templateId: TypeConId,
      key: GlobalKeyWithMaintainers,
      result: Option[ContractId],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyAction
      with CidContainer[LookupByKey] {

    override def keyOpt: Some[GlobalKeyWithMaintainers] = Some(key)

    def gkey: GlobalKey = key.globalKey

    override def mapCid(f: ContractId => ContractId): Node.LookupByKey =
      copy(result = result.map(f))

    def keyMaintainers: Set[Party] = key.maintainers

    override def byKey: Boolean = true

    override private[lf] def updateVersion(version: TransactionVersion): Node.LookupByKey =
      copy(version = version, key = rehash(key))

    override def packageIds: Iterable[PackageId] = Iterable(templateId.packageId)

    final def informeesOfNode: Set[Party] =
      // TODO(JM): In the successful case the informees should be the
      // signatories of the fetch contract. The signatories should be
      // added to the LookupByKey node, or a successful lookup should
      // become a Fetch.
      keyMaintainers
    def requiredAuthorizers: Set[Party] = keyMaintainers
  }

  @deprecated("use GlobalKey", since = "2.6.0")
  type KeyWithMaintainers = GlobalKey
  @deprecated("use GlobalKey", since = "2.6.0")
  val KeyWithMaintainers = GlobalKey

  @deprecated("use VersionedGlobalKey", since = "2.6.0")
  type VersionedKeyWithMaintainers = VersionedGlobalKey

  final case class Rollback(
      children: ImmArray[NodeId]
  ) extends Node
      with CidContainer[Rollback] {

    override def mapCid(f: ContractId => ContractId): Node.Rollback = this
    override def mapNodeId(f: NodeId => NodeId): Node.Rollback =
      copy(children.map(f))
  }

  private def rehash(gk: GlobalKeyWithMaintainers) =
    GlobalKeyWithMaintainers.assertBuild(
      gk.globalKey.templateId,
      gk.value,
      gk.maintainers,
      gk.globalKey.packageName,
    )

}

final case class NodeId(index: Int)
