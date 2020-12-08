// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, ScalazEqual}
import com.daml.lf.value._
import scalaz.Equal
import scalaz.std.option._
import scalaz.syntax.equal._

import scala.language.higherKinds

/**
  * Generic transaction node type for both update transactions and the
  * transaction graph.
  */
object Node {

  /** Transaction nodes parametrized over identifier type */
  sealed trait GenNode[+Nid, +Cid, +Val]
      extends Product
      with Serializable
      with NodeInfo
      with CidContainer[GenNode[Nid, Cid, Val]] {

    def templateId: TypeConName
    def version: TransactionVersion

    final override protected def self: this.type = this

    @deprecated("use resolveRelCid/ensureNoCid/ensureNoRelCid", since = "0.13.52")
    final def mapContractIdAndValue[Cid2, Val2](
        f: Cid => Cid2,
        g: Val => Val2): GenNode[Nid, Cid2, Val2] =
      GenNode.map3(identity[Nid], f, g)(this)
    final def mapNodeId[Nid2](f: Nid => Nid2): GenNode[Nid2, Cid, Val] =
      GenNode.map3(f, identity[Cid], identity[Val])(this)

    /** Required authorizers (see ledger model); UNSAFE TO USE on fetch nodes of transaction with versions < 5
      *
      * The ledger model defines the fetch node actors as the nodes' required authorizers.
      * However, the our transaction data structure did not include the actors in versions < 5.
      * The usage of this method must thus be restricted to:
      * 1. settings where no fetch nodes appear (for example, the `validate` method of DAMLe, which uses it on root
      *    nodes, which are guaranteed never to contain a fetch node)
      * 2. DAML ledger implementations that do not store or process any transactions with version < 5
      *
      */
    def requiredAuthorizers: Set[Party]

    def byKey: Boolean

    def foreach3(fNid: Nid => Unit, fCid: Cid => Unit, fVal: Val => Unit) =
      GenNode.foreach3(fNid, fCid, fVal)(this)

    private[lf] def updateVersion(version: TransactionVersion): GenNode[Nid, Cid, Val]
  }

  object GenNode extends WithTxValue3[GenNode] with CidContainer3[GenNode] {

    override private[lf] def map3[A1, A2, A3, B1, B2, B3](
        f1: A1 => B1,
        f2: A2 => B2,
        f3: A3 => B3,
    ): GenNode[A1, A2, A3] => GenNode[B1, B2, B3] = {
      case self @ NodeCreate(
            coid,
            coinst,
            _,
            _,
            _,
            key,
            _,
          ) =>
        self copy (
          coid = f2(coid),
          coinst = Value.ContractInst.map1(f3)(coinst),
          key = key.map(KeyWithMaintainers.map1(f3)),
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
        self copy (
          coid = f2(coid),
          key = key.map(KeyWithMaintainers.map1(f3)),
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
        self copy (
          targetCoid = f2(targetCoid),
          chosenValue = f3(chosenValue),
          children = children.map(f1),
          exerciseResult = exerciseResult.map(f3),
          key = key.map(KeyWithMaintainers.map1(f3)),
        )
      case self @ NodeLookupByKey(
            _,
            _,
            key,
            result,
            _,
          ) =>
        self copy (
          key = KeyWithMaintainers.map1(f3)(key),
          result = result.map(f2),
        )
    }

    override private[lf] def foreach3[A, B, C](
        f1: A => Unit,
        f2: B => Unit,
        f3: C => Unit,
    ): GenNode[A, B, C] => Unit = {
      case NodeCreate(
          coid,
          coinst,
          optLocation @ _,
          signatories @ _,
          stakeholders @ _,
          key,
          _,
          ) =>
        f2(coid)
        Value.ContractInst.foreach1(f3)(coinst)
        key.foreach(KeyWithMaintainers.foreach1(f3))
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
        key.foreach(KeyWithMaintainers.foreach1(f3))
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
        f3(chosenValue)
        exerciseResult.foreach(f3)
        key.foreach(KeyWithMaintainers.foreach1(f3))
        children.foreach(f1)
      case NodeLookupByKey(
          templateId @ _,
          optLocation @ _,
          key,
          result,
          _,
          ) =>
        KeyWithMaintainers.foreach1(f3)(key)
        result.foreach(f2)
    }
  }

  /** A transaction node that can't possibly refer to `Nid`s. */
  sealed trait LeafOnlyNode[+Cid, +Val] extends GenNode[Nothing, Cid, Val]

  object LeafOnlyNode extends WithTxValue2[LeafOnlyNode]

  /** Denotes the creation of a contract instance. */
  final case class NodeCreate[+Cid, +Val](
      coid: Cid,
      coinst: Value.ContractInst[Val],
      optLocation: Option[Location], // Optional location of the create expression
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[KeyWithMaintainers[Val]],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyNode[Cid, Val]
      with NodeInfo.Create {

    override def templateId: TypeConName = coinst.template
    override def byKey: Boolean = false

    override private[lf] def updateVersion(version: TransactionVersion): NodeCreate[Cid, Val] =
      copy(version = version)
  }

  object NodeCreate extends WithTxValue2[NodeCreate]

  /** Denotes that the contract identifier `coid` needs to be active for the transaction to be valid. */
  final case class NodeFetch[+Cid, +Val](
      coid: Cid,
      override val templateId: TypeConName,
      optLocation: Option[Location], // Optional location of the fetch expression
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[KeyWithMaintainers[Val]],
      override val byKey: Boolean, // invariant (!byKey || exerciseResult.isDefined)
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyNode[Cid, Val]
      with NodeInfo.Fetch {

    override private[lf] def updateVersion(version: TransactionVersion): NodeFetch[Cid, Val] =
      copy(version = version)
  }

  object NodeFetch extends WithTxValue2[NodeFetch]

  /** Denotes a transaction node for an exercise.
    * We remember the `children` of this `NodeExercises`
    * to allow segregating the graph afterwards into party-specific
    * ledgers.
    */
  final case class NodeExercises[+Nid, +Cid, +Val](
      targetCoid: Cid,
      override val templateId: TypeConName,
      choiceId: ChoiceName,
      optLocation: Option[Location], // Optional location of the exercise expression
      consuming: Boolean,
      actingParties: Set[Party],
      chosenValue: Val,
      stakeholders: Set[Party],
      signatories: Set[Party],
      choiceObservers: Set[Party],
      children: ImmArray[Nid],
      exerciseResult: Option[Val],
      key: Option[KeyWithMaintainers[Val]],
      override val byKey: Boolean, // invariant (!byKey || exerciseResult.isDefined)
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends GenNode[Nid, Cid, Val]
      with NodeInfo.Exercise {
    @deprecated("use actingParties instead", since = "1.1.2")
    private[daml] def controllers: actingParties.type = actingParties

    override private[lf] def updateVersion(
        version: TransactionVersion,
    ): NodeExercises[Nid, Cid, Val] =
      copy(version = version)
  }

  object NodeExercises extends WithTxValue3[NodeExercises]

  final case class NodeLookupByKey[+Cid, +Val](
      override val templateId: TypeConName,
      optLocation: Option[Location],
      key: KeyWithMaintainers[Val],
      result: Option[Cid],
      // For the sake of consistency between types with a version field, keep this field the last.
      override val version: TransactionVersion,
  ) extends LeafOnlyNode[Cid, Val]
      with NodeInfo.LookupByKey {

    override def keyMaintainers: Set[Party] = key.maintainers
    override def hasResult: Boolean = result.isDefined
    override def byKey: Boolean = true

    override private[lf] def updateVersion(version: TransactionVersion): NodeLookupByKey[Cid, Val] =
      copy(version = version)
  }

  object NodeLookupByKey extends WithTxValue2[NodeLookupByKey]

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
        f: A => B,
    ): KeyWithMaintainers[A] => KeyWithMaintainers[B] =
      x => x.copy(key = f(x.key))

    override private[lf] def foreach1[A](f: A => Unit): KeyWithMaintainers[A] => Unit =
      x => f(x.key)

  }

  @deprecated("this method is not maintain anymore", since = "1.7.0")
  final def isReplayedBy[Cid: Equal, Val: Equal](
      recorded: GenNode[Nothing, Cid, Val],
      isReplayedBy: GenNode[Nothing, Cid, Val],
  ): Boolean =
    ScalazEqual.match2[recorded.type, isReplayedBy.type, Boolean](fallback = false) {
      case nc: NodeCreate[Cid, Val] => {
        case NodeCreate(
            coid2,
            coinst2,
            optLocation2 @ _,
            signatories2,
            stakeholders2,
            key2,
            version2) =>
          import nc._
          // NOTE(JM): Do not compare location annotations as they may differ due to
          // differing update expression constructed from the root node.
          version == version2 &&
          coid === coid2 && coinst === coinst2 &&
          signatories == signatories2 && stakeholders == stakeholders2 && key === key2
        case _ => false
      }
      case nf: NodeFetch[Cid, Val] => {
        case NodeFetch(
            coid2,
            templateId2,
            optLocation2 @ _,
            actingParties2,
            signatories2,
            stakeholders2,
            key2,
            _,
            version2,
            ) =>
          import nf._
          version == version2 &&
          coid === coid2 && templateId == templateId2 &&
          actingParties.forall(_ => actingParties == actingParties2) &&
          signatories == signatories2 && stakeholders == stakeholders2 &&
          key.forall(_ => key == key2)
      }
      case ne: NodeExercises[Nothing, Cid, Val] => {
        case NodeExercises(
            targetCoid2,
            templateId2,
            choiceId2,
            optLocation2 @ _,
            consuming2,
            actingParties2,
            chosenValue2,
            stakeholders2,
            signatories2,
            choiceObservers2,
            _,
            exerciseResult2,
            key2,
            _,
            version2,
            ) =>
          import ne._
          version == version2 &&
          targetCoid === targetCoid2 && templateId == templateId2 && choiceId == choiceId2 &&
          consuming == consuming2 && actingParties == actingParties2 && chosenValue === chosenValue2 &&
          stakeholders == stakeholders2 && signatories == signatories2 && choiceObservers == choiceObservers2 &&
          exerciseResult.fold(true)(_ => exerciseResult === exerciseResult2) &&
          key.fold(true)(_ => key === key2)
      }
      case nl: NodeLookupByKey[Cid, Val] => {
        case NodeLookupByKey(templateId2, optLocation2 @ _, key2, result2, version2) =>
          import nl._
          version == version2 &&
          templateId == templateId2 &&
          key === key2 && result === result2
      }
    }(recorded, isReplayedBy)

  sealed trait WithTxValue2[F[+ _, + _]] {
    type WithTxValue[+Cid] = F[Cid, Transaction.Value[Cid]]
  }

  sealed trait WithTxValue3[F[+ _, + _, + _]] {
    type WithTxValue[+Nid, +Cid] = F[Nid, Cid, Transaction.Value[Cid]]
  }

}

final case class NodeId(index: Int)

object NodeId {
  implicit def cidMapperInstance[In, Out]: CidMapper[NodeId, NodeId, In, Out] =
    CidMapper.trivialMapper
}
