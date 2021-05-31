// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, ScalazEqual}
import com.daml.lf.value.Value.VersionedValue
import com.daml.lf.value._
import scalaz.Equal
import scalaz.syntax.equal._

/** Generic transaction node type for both update transactions and the
  * transaction graph.
  */
object Node {

  sealed abstract class GenNode[+Nid, +Cid]
      extends Product
      with Serializable
      with CidContainer[GenNode[Nid, Cid]] {

    def foreach2(fNid: Nid => Unit, fCid: Cid => Unit): Unit

    def optVersion: Option[TransactionVersion] = this match {
      case node: GenActionNode[_, _] => Some(node.version)
      case _: NodeRollback[_] => None
    }
  }

  object GenNode extends CidContainer2[GenNode] {
    override private[lf] def map2[A1, B1, A2, B2](
        f1: A1 => A2,
        f2: B1 => B2,
    ): GenNode[A1, B1] => GenNode[A2, B2] = {
      case action: GenActionNode[A1, B1] =>
        GenActionNode.map2(f1, f2)(action)
      case rollback: NodeRollback[A1] =>
        rollback.copy(children = rollback.children.map(f1))
    }

    override private[lf] def foreach2[A, B](f1: A => Unit, f2: B => Unit): GenNode[A, B] => Unit = {
      case action: GenActionNode[A, B] =>
        GenActionNode.foreach2(f1, f2)(action)
      case rollback: NodeRollback[A] =>
        rollback.foreach2(f1, f2)
    }
  }

  /** action nodes parametrized over identifier type */
  sealed abstract class GenActionNode[+Nid, +Cid]
      extends GenNode[Nid, Cid]
      with ActionNodeInfo
      with CidContainer[GenActionNode[Nid, Cid]] {

    def version: TransactionVersion

    private[lf] def updateVersion(version: TransactionVersion): GenNode[Nid, Cid]

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

    def foreach2(fNid: Nid => Unit, fCid: Cid => Unit): Unit =
      GenActionNode.foreach2(fNid, fCid)(this)

    protected def versionValue[Cid2 >: Cid](v: Value[Cid2]): VersionedValue[Cid2] =
      VersionedValue(version, v)
  }

  object GenActionNode extends CidContainer2[GenActionNode] {

    override private[lf] def map2[A1, A2, B1, B2](
        f1: A1 => B1,
        f2: A2 => B2,
    ): GenActionNode[A1, A2] => GenActionNode[B1, B2] = {
      case self @ NodeCreate(
            coid,
            _,
            arg,
            _,
            _,
            _,
            _,
            key,
            _,
          ) =>
        self.copy(
          coid = f2(coid),
          arg = Value.map1(f2)(arg),
          key = key.map(KeyWithMaintainers.map1(Value.map1(f2))),
        )
      case self @ NodeFetch(
            coid,
            _,
            _,
            _,
            _,
            _,
            key,
            _,
            _,
          ) =>
        self.copy(
          coid = f2(coid),
          key = key.map(KeyWithMaintainers.map1(Value.map1(f2))),
        )
      case self @ NodeExercises(
            targetCoid,
            _,
            _,
            _,
            _,
            _,
            chosenValue,
            _,
            _,
            _,
            children,
            exerciseResult,
            key,
            _,
            _,
          ) =>
        self.copy(
          targetCoid = f2(targetCoid),
          chosenValue = Value.map1(f2)(chosenValue),
          children = children.map(f1),
          exerciseResult = exerciseResult.map(Value.map1(f2)),
          key = key.map(KeyWithMaintainers.map1(Value.map1(f2))),
        )
      case self @ NodeLookupByKey(
            _,
            _,
            key,
            result,
            _,
          ) =>
        self.copy(
          key = KeyWithMaintainers.map1(Value.map1(f2))(key),
          result = result.map(f2),
        )
    }

    override private[lf] def foreach2[A, B](
        f1: A => Unit,
        f2: B => Unit,
    ): GenActionNode[A, B] => Unit = {
      case NodeCreate(
            coid,
            templateI @ _,
            arg,
            agreementText @ _,
            optLocation @ _,
            signatories @ _,
            stakeholders @ _,
            key,
            _,
          ) =>
        f2(coid)
        Value.foreach1(f2)(arg)
        key.foreach(KeyWithMaintainers.foreach1(Value.foreach1(f2)))
      case NodeFetch(
            coid,
            templateId @ _,
            optLocationd @ _,
            actingPartiesd @ _,
            signatoriesd @ _,
            stakeholdersd @ _,
            key,
            _,
            _,
          ) =>
        f2(coid)
        key.foreach(KeyWithMaintainers.foreach1(Value.foreach1(f2)))
      case NodeExercises(
            targetCoid,
            templateId @ _,
            choiceId @ _,
            optLocation @ _,
            consuming @ _,
            actingParties @ _,
            chosenValue,
            stakeholders @ _,
            signatories @ _,
            choiceObservers @ _,
            children @ _,
            exerciseResult,
            key,
            _,
            _,
          ) =>
        f2(targetCoid)
        Value.foreach1(f2)(chosenValue)
        exerciseResult.foreach(Value.foreach1(f2))
        key.foreach(KeyWithMaintainers.foreach1(Value.foreach1(f2)))
        children.foreach(f1)
      case NodeLookupByKey(
            templateId @ _,
            optLocation @ _,
            key,
            result,
            _,
          ) =>
        KeyWithMaintainers.foreach1(Value.foreach1(f2))(key)
        result.foreach(f2)
    }
  }

  /** A transaction node that can't possibly refer to `Nid`s. */
  sealed trait LeafOnlyActionNode[+Cid] extends GenActionNode[Nothing, Cid]

  /** Denotes the creation of a contract instance. */
  final case class NodeCreate[+Cid](
      coid: Cid,
      override val templateId: TypeConName,
      arg: Value[Cid],
      agreementText: String,
      optLocation: Option[Location], // Optional location of the create expression
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[KeyWithMaintainers[Value[Cid]]],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyActionNode[Cid]
      with ActionNodeInfo.Create {

    override def byKey: Boolean = false

    override private[lf] def updateVersion(version: TransactionVersion): NodeCreate[Cid] =
      copy(version = version)

    def coinst: Value.ContractInst[Value[Cid]] =
      Value.ContractInst(templateId, arg, agreementText)

    def versionedCoinst: Value.ContractInst[Value.VersionedValue[Cid]] =
      Value.ContractInst(templateId, versionValue(arg), agreementText)

    def versionedKey: Option[KeyWithMaintainers[Value.VersionedValue[Cid]]] =
      key.map(KeyWithMaintainers.map1(versionValue))
  }

  /** Denotes that the contract identifier `coid` needs to be active for the transaction to be valid. */
  final case class NodeFetch[+Cid](
      coid: Cid,
      override val templateId: TypeConName,
      optLocation: Option[Location], // Optional location of the fetch expression
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[KeyWithMaintainers[Value[Cid]]],
      override val byKey: Boolean, // invariant (!byKey || exerciseResult.isDefined)
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyActionNode[Cid]
      with ActionNodeInfo.Fetch {

    override private[lf] def updateVersion(version: TransactionVersion): NodeFetch[Cid] =
      copy(version = version)

    def versionedKey: Option[KeyWithMaintainers[Value.VersionedValue[Cid]]] =
      key.map(KeyWithMaintainers.map1(versionValue))
  }

  /** Denotes a transaction node for an exercise.
    * We remember the `children` of this `NodeExercises`
    * to allow segregating the graph afterwards into party-specific
    * ledgers.
    */
  final case class NodeExercises[+Nid, +Cid](
      targetCoid: Cid,
      override val templateId: TypeConName,
      choiceId: ChoiceName,
      optLocation: Option[Location], // Optional location of the exercise expression
      consuming: Boolean,
      actingParties: Set[Party],
      chosenValue: Value[Cid],
      stakeholders: Set[Party],
      signatories: Set[Party],
      choiceObservers: Set[Party],
      children: ImmArray[Nid],
      exerciseResult: Option[Value[Cid]],
      key: Option[KeyWithMaintainers[Value[Cid]]],
      override val byKey: Boolean, // invariant (!byKey || exerciseResult.isDefined)
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends GenActionNode[Nid, Cid]
      with ActionNodeInfo.Exercise {
    @deprecated("use actingParties instead", since = "1.1.2")
    private[daml] def controllers: actingParties.type = actingParties

    override private[lf] def updateVersion(
        version: TransactionVersion
    ): NodeExercises[Nid, Cid] =
      copy(version = version)

    def versionedChosenValue: Value.VersionedValue[Cid] =
      versionValue(chosenValue)

    def versionedExerciseResult: Option[Value.VersionedValue[Cid]] =
      exerciseResult.map(versionValue)

    def versionedKey: Option[KeyWithMaintainers[Value.VersionedValue[Cid]]] =
      key.map(KeyWithMaintainers.map1(versionValue))
  }

  final case class NodeLookupByKey[+Cid](
      override val templateId: TypeConName,
      optLocation: Option[Location],
      key: KeyWithMaintainers[Value[Cid]],
      result: Option[Cid],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyActionNode[Cid]
      with ActionNodeInfo.LookupByKey {

    override def keyMaintainers: Set[Party] = key.maintainers
    override def hasResult: Boolean = result.isDefined
    override def byKey: Boolean = true

    override private[lf] def updateVersion(version: TransactionVersion): NodeLookupByKey[Cid] =
      copy(version = version)

    def versionedKey: KeyWithMaintainers[Value.VersionedValue[Cid]] =
      KeyWithMaintainers.map1[Value[Cid], Value.VersionedValue[Cid]](versionValue)(key)
  }

  final case class KeyWithMaintainers[+Val](key: Val, maintainers: Set[Party])
      extends CidContainer[KeyWithMaintainers[Val]] {

    override protected def self: this.type = this

    @deprecated("Use resolveRelCid/ensureNoCid/ensureNoRelCid", since = "0.13.52")
    def mapValue[Val1](f: Val => Val1): KeyWithMaintainers[Val1] =
      KeyWithMaintainers.map1(f)(this)

    def foreach1(f: Val => Unit): Unit =
      KeyWithMaintainers.foreach1(f)(this)
  }

  object KeyWithMaintainers extends CidContainer1[KeyWithMaintainers] {
    implicit def equalInstance[Val: Equal]: Equal[KeyWithMaintainers[Val]] =
      ScalazEqual.withNatural(Equal[Val].equalIsNatural) { (a, b) =>
        import a._
        val KeyWithMaintainers(bKey, bMaintainers) = b
        key === bKey && maintainers == bMaintainers
      }

    override private[lf] def map1[A, B](
        f: A => B
    ): KeyWithMaintainers[A] => KeyWithMaintainers[B] =
      x => x.copy(key = f(x.key))

    override private[lf] def foreach1[A](f: A => Unit): KeyWithMaintainers[A] => Unit =
      x => f(x.key)

  }

  final case class NodeRollback[+Nid](
      children: ImmArray[Nid]
  ) extends GenNode[Nid, Nothing] {

    override def foreach2(fNid: Nid => Unit, fCid: Nothing => Unit): Unit =
      children.foreach(fNid)

    override protected def self: GenNode[Nid, Nothing] = this
  }

}

final case class NodeId(index: Int)

object NodeId {
  implicit def cidMapperInstance[In, Out]: CidMapper[NodeId, NodeId, In, Out] =
    CidMapper.trivialMapper
}
