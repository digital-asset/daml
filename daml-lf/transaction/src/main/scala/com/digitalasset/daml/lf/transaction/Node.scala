// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{ImmArray, ScalazEqual}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractInst

import scala.language.higherKinds
import scalaz.Equal
import scalaz.std.option._
import scalaz.syntax.equal._

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
      with value.CidContainer[GenNode[Nid, Cid, Val]] {

    final override protected val self: this.type = this

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
  }

  object GenNode
      extends WithTxValue3[GenNode]
      with value.CidContainer3WithDefaultCidResolver[GenNode] {
    override private[lf] def map3[A1, A2, A3, B1, B2, B3](
        f1: A1 => B1,
        f2: A2 => B2,
        f3: A3 => B3,
    ): GenNode[A1, A2, A3] => GenNode[B1, B2, B3] = {
      case NodeCreate(
          nodeSeed,
          coid,
          coinst,
          optLocation,
          signatories,
          stakeholders,
          key,
          ) =>
        NodeCreate(
          nodeSeed = nodeSeed,
          coid = f2(coid),
          coinst = value.Value.ContractInst.map1(f3)(coinst),
          optLocation = optLocation,
          signatories = signatories,
          stakeholders = stakeholders,
          key = key.map(KeyWithMaintainers.map1(f3)),
        )
      case NodeFetch(
          coid,
          templateId,
          optLocation,
          actingParties,
          signatories,
          stakeholders,
          ) =>
        NodeFetch(
          coid = f2(coid),
          templateId = templateId,
          optLocation = optLocation,
          actingParties = actingParties,
          signatories = signatories,
          stakeholders = stakeholders,
        )
      case NodeExercises(
          nodeSeed,
          targetCoid,
          templateId,
          choiceId,
          optLocation,
          consuming,
          actingParties,
          chosenValue,
          stakeholders,
          signatories,
          controllers,
          children,
          exerciseResult,
          key,
          ) =>
        NodeExercises(
          nodeSeed = nodeSeed,
          targetCoid = f2(targetCoid),
          templateId = templateId,
          choiceId = choiceId,
          optLocation = optLocation,
          consuming = consuming,
          actingParties = actingParties,
          chosenValue = f3(chosenValue),
          stakeholders = stakeholders,
          signatories = signatories,
          controllers = controllers,
          children = children.map(f1),
          exerciseResult = exerciseResult.map(f3),
          key = key.map(KeyWithMaintainers.map1(f3)),
        )
      case NodeLookupByKey(
          templateId,
          optLocation,
          key,
          result,
          ) =>
        NodeLookupByKey(
          templateId = templateId,
          optLocation = optLocation,
          key = KeyWithMaintainers.map1(f3)(key),
          result = result.map(f2),
        )
    }
  }

  /** A transaction node that can't possibly refer to `Nid`s. */
  sealed trait LeafOnlyNode[+Cid, +Val] extends GenNode[Nothing, Cid, Val]

  object LeafOnlyNode extends WithTxValue2[LeafOnlyNode]

  /** Denotes the creation of a contract instance. */
  final case class NodeCreate[+Cid, +Val](
      nodeSeed: Option[crypto.Hash],
      coid: Cid,
      coinst: ContractInst[Val],
      optLocation: Option[Location], // Optional location of the create expression
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[KeyWithMaintainers[Val]],
  ) extends LeafOnlyNode[Cid, Val]
      with NodeInfo.Create {}

  object NodeCreate extends WithTxValue2[NodeCreate]

  /** Denotes that the contract identifier `coid` needs to be active for the transaction to be valid. */
  final case class NodeFetch[+Cid](
      coid: Cid,
      templateId: Identifier,
      optLocation: Option[Location], // Optional location of the fetch expression
      actingParties: Option[Set[Party]],
      signatories: Set[Party],
      stakeholders: Set[Party],
  ) extends LeafOnlyNode[Cid, Nothing]
      with NodeInfo.Fetch {}

  /** Denotes a transaction node for an exercise.
    * We remember the `children` of this `NodeExercises`
    * to allow segregating the graph afterwards into party-specific
    * ledgers.
    */
  final case class NodeExercises[+Nid, +Cid, +Val](
      nodeSeed: Option[crypto.Hash],
      targetCoid: Cid,
      templateId: Identifier,
      choiceId: ChoiceName,
      optLocation: Option[Location], // Optional location of the exercise expression
      consuming: Boolean,
      actingParties: Set[Party],
      chosenValue: Val,
      stakeholders: Set[Party],
      signatories: Set[Party],
      /** Note that here we have both actors and controllers because we use this
        * data structure _before_ we validate the transaction. However for every
        * valid transaction the actors must be the same as the controllers. This
        * is why we removed the controllers field in transaction version 6.
        */
      controllers: Set[Party],
      children: ImmArray[Nid],
      exerciseResult: Option[Val],
      key: Option[KeyWithMaintainers[Val]],
  ) extends GenNode[Nid, Cid, Val]
      with NodeInfo.Exercise {}

  object NodeExercises extends WithTxValue3[NodeExercises] {

    /** After interpretation authorization, it must be the case that
      * the controllers are the same as the acting parties. This
      * apply method enforces it.
      */
    def apply[Nid, Cid, Val](
        nodeSeed: Option[crypto.Hash] = None,
        targetCoid: Cid,
        templateId: Identifier,
        choiceId: ChoiceName,
        optLocation: Option[Location],
        consuming: Boolean,
        actingParties: Set[Party],
        chosenValue: Val,
        stakeholders: Set[Party],
        signatories: Set[Party],
        children: ImmArray[Nid],
        exerciseResult: Option[Val],
        key: Option[KeyWithMaintainers[Val]],
    ): NodeExercises[Nid, Cid, Val] =
      NodeExercises(
        nodeSeed,
        targetCoid,
        templateId,
        choiceId,
        optLocation,
        consuming,
        actingParties,
        chosenValue,
        stakeholders,
        signatories,
        actingParties,
        children,
        exerciseResult,
        key,
      )
  }

  final case class NodeLookupByKey[+Cid, +Val](
      templateId: Identifier,
      optLocation: Option[Location],
      key: KeyWithMaintainers[Val],
      result: Option[Cid],
  ) extends LeafOnlyNode[Cid, Val]
      with NodeInfo.LookupByKey {

    override def keyMaintainers: Set[Party] = key.maintainers
    override def hasResult: Boolean = result.isDefined

  }

  object NodeLookupByKey extends WithTxValue2[NodeLookupByKey]

  final case class KeyWithMaintainers[+Val](key: Val, maintainers: Set[Party])
      extends value.CidContainer[KeyWithMaintainers[Val]] {

    override protected val self: KeyWithMaintainers[Val] = this

    @deprecated("Use resolveRelCid/ensureNoCid/ensureNoRelCid", since = "0.13.52")
    def mapValue[Val1](f: Val => Val1): KeyWithMaintainers[Val1] =
      KeyWithMaintainers.map1(f)(this)
  }

  object KeyWithMaintainers extends value.CidContainer1WithDefaultCidResolver[KeyWithMaintainers] {
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
  }

  final def isReplayedBy[Cid: Equal, Val: Equal](
      recorded: GenNode[Nothing, Cid, Val],
      isReplayedBy: GenNode[Nothing, Cid, Val],
  ): Boolean =
    ScalazEqual.match2[recorded.type, isReplayedBy.type, Boolean](fallback = false) {
      case nc: NodeCreate[Cid, Val] => {
        case NodeCreate(_, coid2, coinst2, optLocation2 @ _, signatories2, stakeholders2, key2) =>
          import nc._
          // NOTE(JM): Do not compare location annotations as they may differ due to
          // differing update expression constructed from the root node.
          coid === coid2 && coinst === coinst2 &&
          signatories == signatories2 && stakeholders == stakeholders2 && key === key2
        case _ => false
      }
      case nf: NodeFetch[Cid] => {
        case NodeFetch(
            coid2,
            templateId2,
            optLocation2 @ _,
            actingParties2,
            signatories2,
            stakeholders2,
            ) =>
          import nf._
          coid === coid2 && templateId == templateId2 &&
          actingParties.forall(_ => actingParties == actingParties2) &&
          signatories == signatories2 && stakeholders == stakeholders2
      }
      case ne: NodeExercises[Nothing, Cid, Val] => {
        case NodeExercises(
            _,
            targetCoid2,
            templateId2,
            choiceId2,
            optLocation2 @ _,
            consuming2,
            actingParties2,
            chosenValue2,
            stakeholders2,
            signatories2,
            controllers2,
            _,
            exerciseResult2,
            key2,
            ) =>
          import ne._
          targetCoid === targetCoid2 && templateId == templateId2 && choiceId == choiceId2 &&
          consuming == consuming2 && actingParties == actingParties2 && chosenValue === chosenValue2 &&
          stakeholders == stakeholders2 && signatories == signatories2 && controllers == controllers2 &&
          exerciseResult.fold(true)(_ => exerciseResult === exerciseResult2) &&
          key.fold(true)(_ => key === key2)
      }
      case nl: NodeLookupByKey[Cid, Val] => {
        case NodeLookupByKey(templateId2, optLocation2 @ _, key2, result2) =>
          import nl._
          templateId == templateId2 &&
          key === key2 && result === result2
      }
    }(recorded, isReplayedBy)

  /** Useful in various circumstances -- basically this is what a ledger implementation must use as
    * a key. The 'hash' is guaranteed to be stable over time.
    */
  final class GlobalKey private (
      val templateId: Identifier,
      val key: Value[Nothing],
      val hash: Hash
  ) extends {
    override def equals(obj: Any): Boolean = obj match {
      case that: GlobalKey => this.hash == that.hash
      case _ => false
    }

    override def hashCode(): Int = hash.hashCode()
  }

  object GlobalKey {
    def apply(templateId: Identifier, key: Value[Nothing]): GlobalKey =
      new GlobalKey(templateId, key, Hash.safeHashContractKey(templateId, key))
  }

  sealed trait WithTxValue2[F[+ _, + _]] {
    type WithTxValue[+Cid] = F[Cid, Transaction.Value[Cid]]
  }

  sealed trait WithTxValue3[F[+ _, + _, + _]] {
    type WithTxValue[+Nid, +Cid] = F[Nid, Cid, Transaction.Value[Cid]]
  }
}
